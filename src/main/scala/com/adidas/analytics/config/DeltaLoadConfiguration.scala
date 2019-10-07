package com.adidas.analytics.config

import com.adidas.analytics.algo.core.Algorithm.{ReadOperation, SafeWriteOperation}
import com.adidas.analytics.config.shared.{ConfigurationContext, DateComponentDerivationConfiguration}
import com.adidas.analytics.util.DataFormat.ParquetFormat
import com.adidas.analytics.util.{DataFormat, InputReader, LoadMode, OutputWriter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SparkSession}


trait DeltaLoadConfiguration extends ConfigurationContext {

  protected val activeRecordsTable: String = configReader.getAs[String]("active_records_table_lake")
  protected val deltaRecordsTable: Option[String] = configReader.getAsOption[String]("delta_records_table_lake")
  protected val deltaRecordsFilePath: Option[String] = configReader.getAsOption[String]("delta_records_file_path")

  protected val businessKey: Seq[String] = configReader.getAsSeq[String]("business_key")
  protected val technicalKey: Seq[String] = configReader.getAsSeq[String]("technical_key")

  protected val rankingColumnName: String = "DELTA_LOAD_RANK"
  protected val recordModeColumnName: String = "recordmode"
  protected val upsertRecordModes: Seq[String] = Seq("", "N")
  protected val upsertRecordsModesFilterFunction: Row => Boolean = { row: Row =>
    val recordmode = row.getAs[String](recordModeColumnName)
    recordmode == null || recordmode == "" || recordmode == "N"
  }
}


object DeltaLoadConfiguration {

  trait PartitionedDeltaLoadConfiguration extends DeltaLoadConfiguration with DateComponentDerivationConfiguration
    with ReadOperation with SafeWriteOperation {

    protected def spark: SparkSession

    override protected val partitionColumns: Seq[String] = configReader.getAsSeq[String]("partition_columns")
    override protected val partitionSourceColumn: String = configReader.getAs[String]("partition_column")
    override protected val partitionSourceColumnFormat: String = configReader.getAs[String]("partition_column_format")

    private val targetSchema: StructType = spark.table(activeRecordsTable).schema

    override protected val readers: Vector[InputReader] = Vector(
      createDeltaInputReader(deltaRecordsFilePath, deltaRecordsTable),
      InputReader.newTableReader(table = activeRecordsTable)
    )

    override protected val writer: OutputWriter.AtomicWriter = OutputWriter.newTableLocationWriter(
      table = activeRecordsTable,
      format = ParquetFormat(Some(targetSchema)),
      partitionColumns = partitionColumns,
      loadMode = LoadMode.OverwritePartitionsWithAddedColumns
    )
  }

  private def createDeltaInputReader(deltaRecordsFilePath: Option[String], deltaRecordsTable: Option[String]): InputReader = {
    def createInputReaderByPath: InputReader = {
      deltaRecordsFilePath.fold {
        throw new RuntimeException("Unable to create a reader for the delta table: neither delta records path not delta table name is defined")
      } {
        location => InputReader.newFileSystemReader(s"$location*.parquet", DataFormat.ParquetFormat())
      }
    }
    deltaRecordsTable.fold(createInputReaderByPath)(tableName => InputReader.newTableReader(tableName))
  }
}
