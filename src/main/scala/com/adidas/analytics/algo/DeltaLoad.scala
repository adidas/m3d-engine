package com.adidas.analytics.algo

import com.adidas.analytics.algo.DeltaLoad._
import com.adidas.analytics.algo.core.Algorithm
import com.adidas.analytics.algo.shared.DateComponentDerivation
import com.adidas.analytics.config.DeltaLoadConfiguration.PartitionedDeltaLoadConfiguration
import com.adidas.analytics.util.DataFrameUtils._
import com.adidas.analytics.util._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.{Logger, LoggerFactory}

/**
  * Performs merge of active records with delta records.
  */
final class DeltaLoad protected(val spark: SparkSession, val dfs: DFSWrapper, val configLocation: String)
  extends Algorithm with PartitionedDeltaLoadConfiguration with DateComponentDerivation {

  override protected def transform(dataFrames: Vector[DataFrame]): Vector[DataFrame] = {
    val dataFramesWithTargetPartitionsAdded = withDatePartitions(spark, dfs, dataFrames.take(1))
    val deltaRecords = dataFramesWithTargetPartitionsAdded(0).persist(StorageLevel.MEMORY_AND_DISK)

    val activeRecords = dataFrames(1)

    val partitions = deltaRecords.collectPartitions(targetPartitions)
    val isRequiredPartition = DataFrameUtils.buildPartitionsCriteriaMatcherFunc(partitions, activeRecords.schema)

    // Create DataFrame containing full content of partitions that need to be touched
    val activeRecordsTargetPartitions = activeRecords.filter(isRequiredPartition).persist(StorageLevel.MEMORY_AND_DISK)

    // Condense delta set
    val upsertRecords = getUpsertRecords(deltaRecords, activeRecords.columns)

    // active records that will be deleted or updated
    val obsoleteRecords = activeRecordsTargetPartitions
      .join(deltaRecords, businessKey, "leftsemi")
      .selectExpr(activeRecords.columns: _*)

    val result = activeRecordsTargetPartitions // Active records in target partitions
      .except(obsoleteRecords) // Minus obsolete records
      .union(upsertRecords) // Union with updated & new records

    Vector(result)
  }

  /**
    * Creation of a condensed set of records to be inserted/updated
    * @param deltaRecords Dataset[Row] containing all delta records
    * @return Dataset[Row] containing the most recent record to be inserted/updated
    */
  private def getUpsertRecords(deltaRecords: Dataset[Row], resultColumns: Seq[String]): Dataset[Row] = {
    // Create partition window - Partitioning by delta records logical key (i.e. technical key of active records)
    val partitionWindow = Window
      .partitionBy(businessKey.map(col): _*)
      .orderBy(technicalKey.map(component => col(component).desc): _*)

    // Ranking & projection
    val rankedDeltaRecords = deltaRecords
      .withColumn(rankingColumnName, row_number().over(partitionWindow))
      .filter(upsertRecordsModesFilterFunction)

    rankedDeltaRecords
      .filter(rankedDeltaRecords(rankingColumnName) === 1)
      .selectExpr(resultColumns: _*)
  }

  protected def withDatePartitions(spark: SparkSession, dfs: DFSWrapper, dataFrames: Vector[DataFrame]): Vector[DataFrame] = {
    logger.info("Adding partitioning information if needed")
    try {
      dataFrames.map { df =>
        if (df.columns.toSeq.intersect(targetPartitions) != targetPartitions){
          df.transform(withDateComponents(partitionSourceColumn, partitionSourceColumnFormat, targetPartitions))
        }
        else df
      }
    } catch {
      case e: Throwable =>
        logger.error("Cannot add partitioning information for data frames.", e)
        //TODO: Handle failure case properly
        throw new RuntimeException("Unable to transform data frames.", e)
    }
  }
}


object DeltaLoad {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def apply(spark: SparkSession, dfs: DFSWrapper, configLocation: String): DeltaLoad = {
    new DeltaLoad(spark, dfs, configLocation)
  }
}