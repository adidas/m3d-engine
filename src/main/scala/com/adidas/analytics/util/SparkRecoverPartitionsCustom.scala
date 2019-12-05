package com.adidas.analytics.util

import com.adidas.analytics.algo.core.Metadata
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col

import scala.collection.JavaConversions._

case class SparkRecoverPartitionsCustom(override val tableName: String,
                                        override val targetPartitions: Seq[String]) extends Metadata {

  override def recoverPartitions(outputDataFrame: DataFrame): Unit = {

    val spark: SparkSession = outputDataFrame.sparkSession

    val targetPartitionsColumns: Seq[Column] = targetPartitions.map(partitionString => col(partitionString))

    val distinctPartitions: DataFrame =  outputDataFrame.select(targetPartitionsColumns: _*).distinct

    val sqlStatements: Dataset[String] = generateAddPartitionStatements(distinctPartitions)

    sqlStatements.collectAsList().foreach((statement: String) => spark.sql(statement))
  }

  private def generateAddPartitionStatements(partitionsDataset: DataFrame): Dataset[String] = {
    partitionsDataset.map(row => {
      val partitionStatementValues: Seq[String] = targetPartitions
        .map(partitionString => s"${partitionString}=${getParameterValue(row, partitionString)}")

      s"ALTER TABLE ${tableName} ADD IF NOT EXISTS PARTITION(${partitionStatementValues.mkString(",")})"
    })(Encoders.STRING)
  }

  private def getParameterValue(row: Row, partitionString: String): String =
    createParameterValue(row.get(row.fieldIndex(partitionString)))

  private def createParameterValue(partitionRawValue: Any): String = {
    partitionRawValue match {
      case value: java.lang.Short => value.toString
      case value: java.lang.Integer => value.toString
      case value: scala.Predef.String  => "'" + value + "'"
      case null => throw new Exception("Partition Value is null. No support for null partitions!")
      case value => throw new Exception("Unsupported partition DataType: " + value.getClass)

    }

  }

}
