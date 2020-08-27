package com.adidas.analytics.algo.core

import com.adidas.analytics.util.DataFrameUtils.PartitionCriteria
import org.apache.spark.sql._

/** This is a generic trait to use in the algorithms where we want to compute statistics on table
  * and partition level
  */
trait TableStatistics extends PartitionHelpers {

  protected def spark: SparkSession

  /** Will add statistics on partition level using HiveQL statements
    *
    * @param df
    *   dataframe
    * @param targetTable
    *   target table
    * @param targetPartitions
    *   target table partitions
    */
  protected def computeStatisticsForTablePartitions(
      df: DataFrame,
      targetTable: String,
      targetPartitions: Seq[String]
  ): Unit = {
    val distinctPartitions: DataFrame = getDistinctPartitions(df, targetPartitions)

    generateComputePartitionStatements(distinctPartitions, targetTable, targetPartitions)
      .collect()
      .foreach((statement: String) => spark.sql(statement))

  }

  /** Will add statistics on partition level using HiveQL statements, given a set of affected
    * partitions
    *
    * @param targetTable
    *   target table
    * @param affectedPartitions
    *   sequence containing the partitions in the DataFrame
    */
  protected def computeStatisticsForTablePartitions(
      targetTable: String,
      affectedPartitions: Seq[PartitionCriteria]
  ): Unit =
    generateComputePartitionStatements(targetTable, affectedPartitions)
      .foreach((statement: String) => spark.sql(statement))

  /** Will add statistics for all table partitions using HiveQL statements
    *
    * @param targetTable
    *   target table
    */
  protected def computeStatisticsForTablePartitions(targetTable: String): Unit = {
    val partitionSpecs = spark.sql(s"SHOW PARTITIONS $targetTable").collect()
    partitionSpecs.foreach { row =>
      val formattedSpec = row
        .getAs[String]("partition")
        .split('/')
        .map { p =>
          val pSplitted = p.split('=')
          "%s='%s'".format(pSplitted(0), pSplitted(1))
        }
        .mkString(",")

      spark.sql(s"ANALYZE TABLE $targetTable PARTITION($formattedSpec) COMPUTE STATISTICS")
    }
  }

  /** Will add statistics on table level using HiveQL statements
    *
    * @param tableName
    *   table name
    */
  protected def computeStatisticsForTable(tableName: Option[String]): Unit =
    tableName match {
      case Some(table) => spark.sql(s"ANALYZE TABLE $table COMPUTE STATISTICS")
      case None        => Unit
    }

  /** Generates analyze table commands to be submitted per partition
    *
    * @param df
    *   dataframe
    * @param targetTable
    *   target table
    * @param targetPartitions
    *   target table partitions
    * @return
    */
  private def generateComputePartitionStatements(
      df: DataFrame,
      targetTable: String,
      targetPartitions: Seq[String]
  ): Dataset[String] =
    df.map { partitionValue =>
      val partitionStatementValues: Seq[String] = targetPartitions.map(partitionColumn =>
        s"$partitionColumn=${getParameterValue(partitionValue, partitionColumn)}"
      )

      s"ANALYZE TABLE $targetTable PARTITION(${partitionStatementValues.mkString(",")}) COMPUTE STATISTICS"
    }(Encoders.STRING)

  /** Generates analyze table commands to be submitted per partition
    *
    * @param targetTable
    *   target table
    * @param affectedPartitions
    *   target table partitions that were affected by a change so stats need to be recomputed
    * @return
    */
  private def generateComputePartitionStatements(
      targetTable: String,
      affectedPartitions: Seq[PartitionCriteria]
  ): Seq[String] =
    affectedPartitions.map { partitionCriteria =>
      val partitionStatementValues: Seq[String] =
        partitionCriteria.map(partition => s"${partition._1}=${partition._2}")

      s"ANALYZE TABLE $targetTable PARTITION(${partitionStatementValues.mkString(",")}) COMPUTE STATISTICS"
    }

}
