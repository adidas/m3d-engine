package com.adidas.analytics.algo.core

import org.apache.spark.sql.DataFrame


trait Metadata {

  protected val tableName: String
  protected val targetPartitions: Seq[String]

  def recoverPartitions(outputDataFrame: DataFrame): Unit

  def refreshTable(outputDataFrame: DataFrame): Unit =
    outputDataFrame.sparkSession.catalog.refreshTable(tableName)

}

