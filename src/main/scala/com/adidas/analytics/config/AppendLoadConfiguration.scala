package com.adidas.analytics.config

import com.adidas.analytics.algo.core.Algorithm.SafeWriteOperation
import com.adidas.analytics.config.shared.{ConfigurationContext, LoadConfiguration}
import com.adidas.analytics.util.DataFormat.ParquetFormat
import com.adidas.analytics.util.{LoadMode, OutputWriter}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType


trait AppendLoadConfiguration extends ConfigurationContext with LoadConfiguration with SafeWriteOperation {

  protected def spark: SparkSession

  private val regexFilename: Seq[String] = configReader.getAsSeq[String]("regex_filename")

  protected val headerDir: String = configReader.getAs[String]("header_dir")

  protected val columnToRegexPairs: Seq[(String, String)] = partitionColumns zip regexFilename

  protected val targetSchema: StructType = spark.table(targetTable).schema

  override protected val writer: OutputWriter.AtomicWriter = OutputWriter.newTableLocationWriter(
    table = targetTable,
    format = ParquetFormat(Some(targetSchema)),
    partitionColumns = partitionColumns,
    loadMode = LoadMode.OverwritePartitions
  )
}
