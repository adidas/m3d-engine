package com.adidas.analytics.algo.core

import org.apache.spark.sql._
import scala.collection.JavaConversions._

/**
  * This is a generic trait to use in the algorithms where we want
  * to compute statistics on table and partition level
  */
trait TableStatistics extends PartitionHelpers {

  protected def spark: SparkSession

  /**
    * will add statistics on partition level using HiveQL statements
    */
  protected def computeStatisticsForTablePartitions(df: DataFrame,
                                                    targetTable: String,
                                                    targetPartitions: Seq[String]): Unit = {

    val distinctPartitions: DataFrame = getDistinctPartitions(df, targetPartitions)

    generateComputePartitionStatements(distinctPartitions, targetTable, targetPartitions)
      .collectAsList()
      .foreach((statement: String) => spark.sql(statement))

  }

  /**
    * will add statistics on table level using HiveQL statements
    */
  protected def computeStatisticsForTable(tableName: Option[String]): Unit = tableName match {
    case Some(table) => spark.sql(s"ANALYZE TABLE ${table} COMPUTE STATISTICS")
    case None => Unit
  }

  private def generateComputePartitionStatements(df: DataFrame,
                                             targetTable: String,
                                             targetPartitions: Seq[String]): Dataset[String] = {
    df.map(partitionValue => {
      val partitionStatementValues: Seq[String] = targetPartitions
        .map(partitionColumn => s"${partitionColumn}=${getParameterValue(partitionValue, partitionColumn)}")

      s"ANALYZE TABLE ${targetTable} PARTITION(${partitionStatementValues.mkString(",")}) COMPUTE STATISTICS"
    })(Encoders.STRING)
  }

}
