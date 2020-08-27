package com.adidas.analytics.algo.loads

import com.adidas.analytics.algo.core.Algorithm.WriteOperation
import com.adidas.analytics.algo.core.{Algorithm, TableStatistics}
import com.adidas.analytics.algo.loads.FullLoad._
import com.adidas.analytics.algo.shared.{DataReshapingTask, DateComponentDerivation}
import com.adidas.analytics.config.loads.FullLoadConfiguration
import com.adidas.analytics.util.DataFormat.{DSVFormat, JSONFormat, ParquetFormat}
import com.adidas.analytics.util._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

final class FullLoad protected (
    val spark: SparkSession,
    val dfs: DFSWrapper,
    val configLocation: String
) extends Algorithm
    with WriteOperation
    with FullLoadConfiguration
    with DateComponentDerivation
    with TableStatistics
    with DataReshapingTask {

  override protected def read(): Vector[DataFrame] =
    try {
      val dataFormat: DataFormat = fileFormat match {
        case "parquet" => ParquetFormat(Some(targetSchema))
        case "dsv"     => DSVFormat(Some(targetSchema))
        case "json" =>
          JSONFormat(multiLine = isMultilineJSON.getOrElse(false), optionalSchema = readJsonSchema)
        case _ => throw new RuntimeException(s"Unsupported input data format $fileFormat.")
      }
      Vector(dataFormat.read(spark.read.options(sparkReaderOptions), inputDir))
    } catch { case e: Throwable => throw new RuntimeException("Unable to read input data.", e) }

  override protected def transform(dataFrames: Vector[DataFrame]): Vector[DataFrame] =
    try additionalTasksDataFrame(
      spark,
      dataFrames,
      targetSchema,
      partitionSourceColumn,
      partitionSourceColumnFormat,
      targetPartitions
    )
    catch { case e: Throwable => throw new RuntimeException("Unable to transform data frames.", e) }

  override protected def write(dataFrames: Vector[DataFrame]): Vector[DataFrame] = {
    val outputDfs = {
      try super.write(dataFrames)
      catch {
        case e: Throwable =>
          logger.info(
            s"An exception occurred while writing the data... cleaning up temporary files: ${e.getMessage}"
          )
          cleanupDirectory(nextTableLocation)
          throw new RuntimeException("Unable to write DataFrames.", e)
      }
    }

    try CatalogTableManager(targetTable, spark).recreateTable(nextTableLocation, targetPartitions)
    catch {
      case e: Throwable =>
        logger.info(s"An exception occurred while recreating table: ${e.getMessage}")
        cleanupDirectory(nextTableLocation)
        CatalogTableManager(targetTable, spark).recreateTable(
          currentTableLocation,
          targetPartitions
        )
        throw new RuntimeException(s"Unable to recreate table in location: $nextTableLocation", e)
    }

    cleanupDirectory(currentTableLocation)
    cleanupTableLeftovers(tableRootDir, nextTableLocationPrefix)

    outputDfs
  }

  override protected def updateStatistics(dataFrames: Vector[DataFrame]): Unit =
    if (computeTableStatistics && dataType == STRUCTURED) {
      if (targetPartitions.nonEmpty) computeStatisticsForTablePartitions(targetTable)
      computeStatisticsForTable(Some(targetTable))
    }

  private def cleanupDirectory(dir: String): Unit =
    HadoopLoadHelper.cleanupDirectoryContent(dfs, dir)

  private def cleanupTableLeftovers(dir: String, ignorePrefix: String): Unit =
    HadoopLoadHelper.cleanupDirectoryLeftovers(dfs, dir, ignorePrefix)

}

object FullLoad {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def apply(spark: SparkSession, dfs: DFSWrapper, configLocation: String): FullLoad =
    new FullLoad(spark, dfs, configLocation)
}
