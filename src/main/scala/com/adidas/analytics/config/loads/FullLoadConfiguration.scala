package com.adidas.analytics.config.loads

import com.adidas.analytics.algo.core.Algorithm.BaseWriteOperation
import com.adidas.analytics.config.shared.{
  ConfigurationContext,
  DateComponentDerivationConfiguration
}
import com.adidas.analytics.util.DataFormat.ParquetFormat
import com.adidas.analytics.util._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.FailFastMode
import org.apache.spark.sql.types.{DataType, StructType}
import scala.util.parsing.json.JSONObject

trait FullLoadConfiguration
    extends ConfigurationContext
    with LoadConfiguration
    with DateComponentDerivationConfiguration
    with BaseWriteOperation {

  protected def spark: SparkSession

  protected def dfs: DFSWrapper

  protected val targetTable: String = configReader.getAs[String]("target_table")

  protected val baseDataDir: String = configReader.getAs[String]("base_data_dir")

  protected val currentTableLocation: String =
    CatalogTableManager(targetTable, spark).getTableLocation

  protected val tableRootDir: String =
    currentTableLocation.substring(0, currentTableLocation.lastIndexOf('/'))

  protected val nextTableLocation: String =
    HadoopLoadHelper.buildTimestampedTablePath(new Path(tableRootDir, baseDataDir)).toString

  protected val nextTableLocationPrefix: String =
    nextTableLocation.substring(nextTableLocation.lastIndexOf('/'))

  protected val isMultilineJSON: Option[Boolean] =
    configReader.getAsOption[Boolean]("is_multiline_json")

  protected val dropDateDerivedColumns: Boolean =
    configReader
      .getAsOption[Boolean]("drop_date_derived_columns")
      .getOrElse(if (loadMode == FailFastMode.name) true else false)

  protected val targetSchema: StructType = CatalogTableManager(targetTable, spark).getSchemaSafely(
    dfs,
    targetPartitions,
    dropDateDerivedColumns,
    addCorruptRecordColumn,
    Some(loadMode)
  )

  protected val partitionSourceColumn: String = configReader.getAs[String]("partition_column")

  protected val partitionSourceColumnFormat: String =
    configReader.getAs[String]("partition_column_format")

  override protected val outputFilesNum: Option[Int] =
    configReader.getAsOption[Int]("output_files_num").orElse(Some(10))

  protected val writer: OutputWriter.AtomicWriter = dataType match {
    case STRUCTURED =>
      OutputWriter.newFileSystemWriter(
        location = nextTableLocation,
        format = ParquetFormat(Some(targetSchema)),
        targetPartitions = targetPartitions,
        loadMode = LoadMode.OverwriteTable
      )
    case anotherDataType =>
      throw new RuntimeException(s"Unsupported data type: $anotherDataType for FullLoad.")
  }

  protected val readJsonSchema: Option[StructType] =
    configReader.getAsOption[JSONObject]("schema") match {
      case Some(value) => Some(DataType.fromJson(value.toString()).asInstanceOf[StructType])
      case _           => None
    }

  // effectively disable the empty string to null conversion here per default
  override protected def readNullValue: Option[String] =
    super.readNullValue.orElse(Some("XXNULLXXX"))

  override def loadMode: String = readerModeSetter(FailFastMode.name)

}
