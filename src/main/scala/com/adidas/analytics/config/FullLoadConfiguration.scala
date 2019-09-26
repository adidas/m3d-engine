package com.adidas.analytics.config

import com.adidas.analytics.config.shared.{ConfigurationContext, DateComponentDerivationConfiguration, LoadConfiguration}
import com.adidas.analytics.util.DataFormat.ParquetFormat
import com.adidas.analytics.util.{LoadMode, OutputWriter}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.PermissiveMode
import org.apache.spark.sql.types.StructType


trait FullLoadConfiguration extends ConfigurationContext with LoadConfiguration with DateComponentDerivationConfiguration {

  protected def spark: SparkSession

  protected val currentDir: String = configReader.getAs[String]("current_dir")

  protected val backupDir: String = configReader.getAs[String]("backup_dir")

  protected val targetSchema: StructType = spark.table(targetTable).schema

  protected val writer: OutputWriter.AtomicWriter = OutputWriter.newFileSystemWriter(
    location = currentDir,
    format = ParquetFormat(Some(targetSchema)),
    partitionColumns = partitionColumns,
    loadMode = LoadMode.OverwriteTable
  )

  override protected val partitionSourceColumn: String = configReader.getAs[String]("partition_column")
  override protected val partitionSourceColumnFormat: String = configReader.getAs[String]("partition_column_format")

  override protected def loadMode: String = PermissiveMode.name

  override protected def readNullValue: Option[String] = {
    // all views built on top of BI full loads expect to have empty strings instead of null values, so we have to effectively disable the empty string to null conversion here per default (BDE-2256)
    super.readNullValue.orElse(Some("XXNULLXXX"))
  }
}
