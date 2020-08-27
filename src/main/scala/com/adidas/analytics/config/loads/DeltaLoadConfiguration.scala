package com.adidas.analytics.config.loads

import com.adidas.analytics.algo.core.Algorithm.{
  ReadOperation,
  SafeWriteOperation,
  UpdateStatisticsOperation
}
import com.adidas.analytics.config.shared.{
  ConfigurationContext,
  DateComponentDerivationConfiguration,
  MetadataUpdateStrategy
}
import com.adidas.analytics.util.DataFormat.ParquetFormat
import com.adidas.analytics.util._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}

trait DeltaLoadConfiguration
    extends ConfigurationContext
    with UpdateStatisticsOperation
    with MetadataUpdateStrategy {

  protected val activeRecordsTable: String = configReader.getAs[String]("active_records_table_lake")

  protected val deltaRecordsTable: Option[String] =
    configReader.getAsOption[String]("delta_records_table_lake")

  protected val deltaRecordsFilePath: Option[String] =
    configReader.getAsOption[String]("delta_records_file_path")

  protected val businessKey: Seq[String] = configReader.getAsSeq[String]("business_key")

  protected val technicalKey: Seq[String] = configReader.getAsSeq[String]("technical_key")

  protected val rankingColumnName: String = "DELTA_LOAD_RANK"
  protected val recordModeColumnName: String = "recordmode"
  protected val upsertRecordModes: Seq[String] = Seq("", "N")

  protected def upsertRecordsModesFilterFunction: Row => Boolean = { row: Row =>
    var recordmode = ""
    try recordmode = row.getAs[String](recordModeColumnName)
    catch { case _: Throwable => recordmode = row.getAs[String](recordModeColumnName.toUpperCase) }
    recordmode == null || recordmode == "" || recordmode == "N"
  }
}

object DeltaLoadConfiguration {

  trait PartitionedDeltaLoadConfiguration
      extends DeltaLoadConfiguration
      with DateComponentDerivationConfiguration
      with ReadOperation
      with SafeWriteOperation {

    protected def spark: SparkSession

    override protected val targetPartitions: Seq[String] =
      configReader.getAsSeq[String]("target_partitions")

    override protected val partitionSourceColumn: String =
      configReader.getAs[String]("partition_column")

    override protected val partitionSourceColumnFormat: String =
      configReader.getAs[String]("partition_column_format")

    private val targetSchema: StructType =
      CatalogTableManager(activeRecordsTable, spark).getSchemaSafely(dfs)

    override protected val readers: Vector[InputReader] = Vector(
      createDeltaInputReader(deltaRecordsFilePath, deltaRecordsTable),
      InputReader.newTableReader(table = activeRecordsTable)
    )

    override protected val writer: OutputWriter.AtomicWriter = OutputWriter.newTableLocationWriter(
      table = activeRecordsTable,
      format = ParquetFormat(Some(targetSchema)),
      targetPartitions = targetPartitions,
      metadataConfiguration = getMetaDataUpdateStrategy(activeRecordsTable, targetPartitions),
      loadMode = LoadMode.OverwritePartitionsWithAddedColumns
    )
  }

  private def createDeltaInputReader(
      deltaRecordsFilePath: Option[String],
      deltaRecordsTable: Option[String]
  ): InputReader = {
    def createInputReaderByPath: InputReader =
      deltaRecordsFilePath.fold {
        throw new RuntimeException(
          "Unable to create a reader for the delta table: neither delta records path not delta table name is defined"
        )
      }(location => InputReader.newFileSystemReader(location, DataFormat.ParquetFormat()))

    deltaRecordsTable
      .fold(createInputReaderByPath)(tableName => InputReader.newTableReader(tableName))
  }
}
