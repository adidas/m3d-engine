package com.adidas.analytics.util

import com.adidas.analytics.algo.core.{Metadata, PartitionHelpers}
import org.apache.spark.sql._

case class RecoverPartitionsCustom(
    override val tableName: String,
    override val targetPartitions: Seq[String]
) extends Metadata
    with PartitionHelpers {

  override def recoverPartitions(outputDataFrame: DataFrame): Unit = {
    val spark: SparkSession = outputDataFrame.sparkSession

    val distinctPartitions: DataFrame = getDistinctPartitions(outputDataFrame, targetPartitions)

    generateAddPartitionStatements(distinctPartitions)
      .collect()
      .foreach((statement: String) => spark.sql(statement))

  }

  private def generateAddPartitionStatements(df: DataFrame): Dataset[String] =
    df.map { partitionValue =>
      val partitionStatementValues: Seq[String] = targetPartitions.map(partitionColumn =>
        s"$partitionColumn=${getParameterValue(partitionValue, partitionColumn)}"
      )

      s"ALTER TABLE $tableName ADD IF NOT EXISTS PARTITION(${partitionStatementValues.mkString(",")})"
    }(Encoders.STRING)

}
