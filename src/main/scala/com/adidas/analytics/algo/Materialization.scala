package com.adidas.analytics.algo

import com.adidas.analytics.algo.core.Algorithm
import com.adidas.analytics.config.MaterializationConfiguration
import com.adidas.analytics.config.MaterializationConfiguration._
import com.adidas.analytics.config.shared.ConfigurationContext
import com.adidas.analytics.util._
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.slf4j.{Logger, LoggerFactory}

/** Performs loading of partitions from existing view/table to a specified location overwriting
  * existing data
  */
trait Materialization extends Algorithm with MaterializationConfiguration {

  override protected def transform(dataFrames: Vector[DataFrame]): Vector[DataFrame] = {
    val inputDf = if (toCache) dataFrames(0).cache() else dataFrames(0)
    val result = Option(partitionsCriteria).filter(_.nonEmpty).foldLeft(inputDf) {
      case (df, partitionsCriteria) =>
        val isRequiredPartition =
          DataFrameUtils.buildPartitionsCriteriaMatcherFunc(partitionsCriteria, df.schema)
        df.filter(isRequiredPartition)
    }

    Vector(result)
  }
}

object Materialization {

  def newFullMaterialization(
      spark: SparkSession,
      dfs: DFSWrapper,
      configLocation: String
  ): Materialization = new FullMaterialization(spark, dfs, LoadMode.OverwriteTable, configLocation)

  def newRangeMaterialization(
      spark: SparkSession,
      dfs: DFSWrapper,
      configLocation: String
  ): Materialization =
    new RangeMaterialization(
      spark,
      dfs,
      LoadMode.OverwritePartitionsWithAddedColumns,
      configLocation
    )

  def newQueryMaterialization(
      spark: SparkSession,
      dfs: DFSWrapper,
      configLocation: String
  ): Materialization =
    new QueryMaterialization(
      spark,
      dfs,
      LoadMode.OverwritePartitionsWithAddedColumns,
      configLocation
    )

  private class FullMaterialization(
      val spark: SparkSession,
      val dfs: DFSWrapper,
      val loadMode: LoadMode,
      val configLocation: String
  ) extends Materialization
      with FullMaterializationConfiguration
      with ConfigurationContext {

    private val logger: Logger = LoggerFactory.getLogger(getClass)

    override protected def write(dataFrames: Vector[DataFrame]): Vector[DataFrame] = {
      val outputDfs = {
        try dataFrames.map { df =>
          if (targetPartitions.isEmpty)
            writer.write(dfs, outputFilesNum.map(df.repartition).getOrElse(df))
          else {
            val partitionCols = targetPartitions.map(columnName => col(columnName))
            writer.write(
              dfs,
              outputFilesNum
                .map(n => df.repartition(n, partitionCols: _*))
                .getOrElse(df.repartition(partitionCols: _*))
            )
          }
        } catch {
          case e: Throwable =>
            logger.info(
              s"An exception occurred while writing the data... cleaning up temporary files: ${e.getMessage}"
            )
            HadoopLoadHelper.cleanupDirectoryContent(dfs, nextTableLocation.toString)
            throw new RuntimeException("Unable to write data for the materialized view.", e)
        }
      }

      try CatalogTableManager(targetTable, spark)
        .recreateTable(nextTableLocation.toString, targetPartitions)
      catch {
        case e: Throwable =>
          logger.info(s"An exception occurred while recreating materialized view: ${e.getMessage}")
          HadoopLoadHelper.cleanupDirectoryContent(dfs, nextTableLocation.toString)
          CatalogTableManager(targetTable, spark)
            .recreateTable(currentTableLocation.toString, targetPartitions)
          throw new RuntimeException(
            s"Unable to recreate materialized view in location: $nextTableLocation",
            e
          )
      }

      val orderedSubFolders = HadoopLoadHelper.getOrderedSubFolders(
        dfs,
        tableDataDir.toString,
        Some(numVersionsToRetain + 1),
        Some(new FullMaterializationPathFilter())
      )

      HadoopLoadHelper.cleanupDirectoryLeftovers(dfs, tableDataDir.toString, orderedSubFolders)

      outputDfs
    }

    /** A custom path filter to ignore EMR S3 folder placeholders, partition folders and parquet
      * files when listing folders/files using HadoopFS. Such feature is useful to allow ordering
      * timestamped folders containing different versions of the materialized view and keep only the
      * last x versions, without letting leftover partitions or files interfere with the sorting
      * logic.
      */
    private class FullMaterializationPathFilter extends PathFilter {

      override def accept(path: Path): Boolean =
        !sortingIgnoreFolderNames.exists(path.toString.contains)
    }

  }

  private class RangeMaterialization(
      val spark: SparkSession,
      val dfs: DFSWrapper,
      val loadMode: LoadMode,
      val configLocation: String
  ) extends Materialization
      with RangeMaterializationConfiguration
      with ConfigurationContext {}

  private class QueryMaterialization(
      val spark: SparkSession,
      val dfs: DFSWrapper,
      val loadMode: LoadMode,
      val configLocation: String
  ) extends Materialization
      with QueryMaterializationConfiguration
      with ConfigurationContext {}

}
