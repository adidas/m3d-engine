package com.adidas.analytics.util

import com.adidas.analytics.algo.core.Metadata
import org.apache.spark.sql.DataFrame

case class RecoverPartitionsNative(
    override val tableName: String,
    override val targetPartitions: Seq[String]
) extends Metadata {

  override def recoverPartitions(outputDataFrame: DataFrame): Unit =
    outputDataFrame.sparkSession.catalog.recoverPartitions(tableName)

}
