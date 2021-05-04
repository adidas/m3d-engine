package com.adidas.analytics.config.loads

import com.adidas.analytics.algo.core.Algorithm.SafeWriteOperation
import com.adidas.analytics.config.shared.{ConfigurationContext, MetadataUpdateStrategy}
import com.adidas.analytics.util.DataFormat.ParquetFormat
import com.adidas.analytics.util.{CatalogTableManager, LoadMode, OutputWriter}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.DropMalformedMode
import org.apache.spark.sql.types.{DataType, StructType}

import scala.util.parsing.json.JSONObject

trait AppendLoadConfiguration
    extends ConfigurationContext
    with LoadConfiguration
    with SafeWriteOperation
    with MetadataUpdateStrategy {

  protected def spark: SparkSession

  protected val regexFilename: Option[Seq[String]] =
    configReader.getAsOptionSeq[String]("regex_filename")

  protected val partitionSourceColumn: Option[String] =
    configReader.getAsOption[String]("partition_column")

  protected val partitionSourceColumnFormat: Option[String] =
    configReader.getAsOption[String]("partition_column_format")
  protected val headerDir: String = configReader.getAs[String]("header_dir")

  protected val targetTable: Option[String] = configReader.getAsOption[String]("target_table")

  /* This option is used to specify whether the input data schema must be the same as target schema
   * specified in the configuration file */
  /* Note: if it is set to True, it will cause input data to be read more than once */
  private val verifySchemaOption: Option[Boolean] =
    configReader.getAsOption[Boolean]("verify_schema")

  protected val verifySchema: Boolean = dataType match {
    case SEMISTRUCTURED => verifySchemaOption.getOrElse(true)
    case _              => false
  }

  private val jsonSchemaOption: Option[JSONObject] = configReader.getAsOption[JSONObject]("schema")

  protected val dropDateDerivedColumns: Boolean =
    configReader.getAsOption[Boolean]("drop_date_derived_columns").getOrElse(false)

  protected val targetSchema: StructType = getTargetSchema

  private val targetDir: Option[String] = configReader.getAsOption[String]("target_dir")

  private val writeLoadMode: LoadMode =
    configReader.getAsOption[String]("write_load_mode") match {
      case Some("AppendUnionPartitions") => LoadMode.AppendUnionPartitions
      case None                          => LoadMode.OverwritePartitions
    }

  override protected val outputFilesNum: Option[Int] =
    configReader.getAsOption[Int]("output_files_num")

  override protected val writer: OutputWriter.AtomicWriter = dataType match {
    case STRUCTURED if targetTable.isDefined =>
      OutputWriter.newTableLocationWriter(
        table = targetTable.get,
        format = ParquetFormat(Some(targetSchema)),
        targetPartitions = targetPartitions,
        loadMode = writeLoadMode,
        metadataConfiguration = getMetaDataUpdateStrategy(targetTable.get, targetPartitions)
      )
    case SEMISTRUCTURED if targetDir.isDefined =>
      OutputWriter.newFileSystemWriter(
        location = targetDir.get,
        format = ParquetFormat(Some(targetSchema)),
        targetPartitions = targetPartitions,
        loadMode = writeLoadMode
      )
    case anotherDataType =>
      throw new RuntimeException(
        s"Unsupported data type: $anotherDataType in AppendLoad or the configuration file is malformed."
      )
  }

  private def getTargetSchemaFromHiveTable: StructType =
    targetTable match {
      case Some(tableName) =>
        CatalogTableManager(tableName, spark).getSchemaSafely(
          dfs,
          targetPartitions,
          dropDateDerivedColumns,
          addCorruptRecordColumn,
          Some(loadMode)
        )
      case None => throw new RuntimeException("No schema definition found.")
    }

  private def getTargetSchema: StructType =
    dataType match {
      case STRUCTURED => getTargetSchemaFromHiveTable
      case SEMISTRUCTURED if jsonSchemaOption.isDefined =>
        DataType.fromJson(jsonSchemaOption.get.toString()).asInstanceOf[StructType]
      case anotherDataType =>
        throw new RuntimeException(
          s"Unsupported data type: $anotherDataType in AppendLoad or the configuration file is malformed."
        )
    }

  override def loadMode: String = readerModeSetter(DropMalformedMode.name)
}
