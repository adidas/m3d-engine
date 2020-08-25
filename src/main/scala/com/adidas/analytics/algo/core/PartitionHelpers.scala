package com.adidas.analytics.algo.core

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

/** This is a trait with generic logic to interact with dataframes on partition level
  */
trait PartitionHelpers {

  protected def getDistinctPartitions(
      outputDataFrame: DataFrame,
      targetPartitions: Seq[String]
  ): Dataset[Row] = {
    val targetPartitionsColumns: Seq[Column] =
      targetPartitions.map(partitionString => col(partitionString))
    outputDataFrame.select(targetPartitionsColumns: _*).distinct
  }

  protected def getParameterValue(row: Row, partitionString: String): String =
    createParameterValue(row.get(row.fieldIndex(partitionString)))

  protected def createParameterValue(partitionRawValue: Any): String =
    partitionRawValue match {
      case value: java.lang.Short     => value.toString
      case value: java.lang.Integer   => value.toString
      case value: scala.Predef.String => "'" + value + "'"
      case null                       => throw new Exception("Partition Value is null. No support for null partitions!")
      case value                      => throw new Exception("Unsupported partition DataType: " + value.getClass)
    }
}
