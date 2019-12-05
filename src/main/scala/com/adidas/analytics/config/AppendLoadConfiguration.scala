package com.adidas.analytics.config

import com.adidas.analytics.algo.core.Algorithm.SafeWriteOperation
import com.adidas.analytics.config.shared.{ConfigurationContext, LoadConfiguration, MetadataUpdateStrategy}
import com.adidas.analytics.util.DataFormat.ParquetFormat
import com.adidas.analytics.util.{LoadMode, OutputWriter}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.DropMalformedMode
import org.apache.spark.sql.types.{DataType, StructType}

import scala.util.parsing.json.JSONObject


trait AppendLoadConfiguration extends ConfigurationContext
  with LoadConfiguration
  with SafeWriteOperation
  with MetadataUpdateStrategy {

  protected def spark: SparkSession

  private val regexFilename: Seq[String] = configReader.getAsSeq[String]("regex_filename")

  protected val headerDir: String = configReader.getAs[String]("header_dir")

  protected val targetTable: Option[String] = configReader.getAsOption[String]("target_table")


  // This option is used to specify whether the input data schema must be the same as target schema specified in the configuration file
  // Note: if it is set to True, it will cause input data to be read more than once
  private val verifySchemaOption: Option[Boolean] = configReader.getAsOption[Boolean]("verify_schema")

  protected val verifySchema: Boolean = dataType match {
    case SEMISTRUCTURED => verifySchemaOption.getOrElse(true)
    case _ => false
  }

  protected val columnToRegexPairs: Seq[(String, String)] = targetPartitions zip regexFilename

  private val jsonSchemaOption: Option[JSONObject] = configReader.getAsOption[JSONObject]("schema")

  protected val targetSchema: StructType = getTargetSchema

  private val targetDir: Option[String] = configReader.getAsOption[String]("target_dir")

  override protected val writer: OutputWriter.AtomicWriter = dataType match {
    case STRUCTURED if targetTable.isDefined => OutputWriter.newTableLocationWriter(
      table = targetTable.get,
      format = ParquetFormat(Some(targetSchema)),
      targetPartitions = targetPartitions,
      loadMode = LoadMode.OverwritePartitionsWithAddedColumns,
      metadataConfiguration = getMetaDataUpdateStrategy(targetTable.get,targetPartitions)
    )
    case SEMISTRUCTURED if targetDir.isDefined => OutputWriter.newFileSystemWriter(
      location = targetDir.get,
      format = ParquetFormat(Some(targetSchema)),
      targetPartitions = targetPartitions,
      loadMode = LoadMode.OverwritePartitions
    )
    case anotherDataType => throw new RuntimeException(s"Unsupported data type: $anotherDataType in AppendLoad or the configuration file is malformed.")
  }

  private def getTargetSchemaFromHiveTable: StructType = {
    targetTable match {
      case Some(tableName) => spark.table(tableName).schema
      case None => throw new RuntimeException("No schema definition found.")
    }
  }

  private def getTargetSchema: StructType = {
    dataType match {
      case STRUCTURED => getTargetSchemaFromHiveTable
      case SEMISTRUCTURED if jsonSchemaOption.isDefined => DataType.fromJson(jsonSchemaOption.get.toString()).asInstanceOf[StructType]
      case anotherDataType => throw new RuntimeException(s"Unsupported data type: $anotherDataType in AppendLoad or the configuration file is malformed.")
    }
  }

  override def loadMode: String = readerModeSetter(DropMalformedMode.name)
}
