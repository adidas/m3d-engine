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

  protected val targetTable: String = configReader.getAs[String]("target_table")

  protected val targetSchema: StructType = spark.table(targetTable).schema

  protected val writer: OutputWriter.AtomicWriter = dataType match {
    case STRUCTURED => OutputWriter.newFileSystemWriter(
      location = currentDir,
      format = ParquetFormat(Some(targetSchema)),
      targetPartitions = targetPartitions,
      loadMode = LoadMode.OverwriteTable
    )
    case anotherDataType => throw new RuntimeException(s"Unsupported data type: $anotherDataType for FullLoad.")
  }


  override protected val partitionSourceColumn: String = configReader.getAs[String]("partition_column")
  override protected val partitionSourceColumnFormat: String = configReader.getAs[String]("partition_column_format")

  override protected def readNullValue: Option[String] = {
    super.readNullValue.orElse(Some("XXNULLXXX"))
  }

  override def loadMode: String = readerModeSetter(PermissiveMode.name)
}
