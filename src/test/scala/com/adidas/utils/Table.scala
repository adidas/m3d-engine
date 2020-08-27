package com.adidas.utils

import com.adidas.analytics.util.DataFormat.{DSVFormat, ParquetFormat}
import com.adidas.analytics.util._
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructField, StructType}
import org.slf4j.{Logger, LoggerFactory}

case class Table private (
    dfs: DFSWrapper,
    spark: SparkSession,
    table: String,
    location: String,
    schema: StructType,
    targetPartitions: Seq[String],
    format: DataFormat,
    options: Map[String, String]
) {

  def read(): DataFrame = spark.read.table(table)

  def write(
      files: Seq[String],
      reader: FileReader,
      loadMode: LoadMode,
      fillNulls: Boolean = false
  ): Unit = {
    val df = files.map(file => reader.read(spark, file, fillNulls)).reduce(_ union _)
    createWriter(loadMode).write(dfs, df)
    invalidateCaches()
  }

  def write(records: Seq[Map[String, Any]], loadMode: LoadMode, fillNulls: Boolean): Unit = {
    val rows = records.map(map => Table.mapToRow(map, fillNulls, schema))
    val rdd = spark.sparkContext.makeRDD(rows)
    val df = spark.createDataFrame(rdd, schema)
    createWriter(loadMode).write(dfs, df)
    invalidateCaches()
  }

  def write(df: DataFrame, loadMode: LoadMode): Unit = {
    createWriter(loadMode).write(dfs, df)
    invalidateCaches()
  }

  def write(records: Seq[Seq[Any]], loadMode: LoadMode): Unit = {
    val writer = createWriter(loadMode)
    val rdd = spark.sparkContext.makeRDD(records.map(Row.fromSeq))
    val df = spark.createDataFrame(rdd, schema)
    writer.write(dfs, df)
    invalidateCaches()
  }

  private def invalidateCaches(): Unit =
    if (targetPartitions.nonEmpty) spark.catalog.recoverPartitions(table)
    else spark.catalog.refreshTable(table)

  private def createWriter(loadMode: LoadMode): OutputWriter =
    OutputWriter.newFileSystemWriter(
      location,
      format,
      targetPartitions,
      options + ("emptyValue" -> ""),
      loadMode
    )
}

object Table {

  def newBuilder(
      table: String,
      database: String,
      location: String,
      schema: StructType
  ): TableBuilder = new TableBuilder(table, database, location, schema)

  private def mapToRow(rowValues: Map[String, Any], fillNulls: Boolean, schema: StructType): Row = {
    val record = schema.fields.map {
      case StructField(name, _, _, _) => rowValues.getOrElse(name, if (fillNulls) "" else null)
    }
    Row.fromSeq(record)
  }

  protected def buildColumnDefinitions(fields: Seq[StructField]): String =
    fields
      .map { case StructField(name, dataType, _, _) => s"$name ${dataType.typeName}" }
      .mkString(", ")

  class TableBuilder(table: String, database: String, location: String, schema: StructType) {

    protected val logger: Logger = LoggerFactory.getLogger(getClass)

    private val fullTableName: String = s"$database.$table"

    private var targetPartitions: Seq[String] = Seq()

    private val defaultDSVOptions: Map[String, String] = Map("delimiter" -> "|")
    private var options: Map[String, String] = Map()

    def withPartitions(targetPartitions: Seq[String]): TableBuilder = {
      this.targetPartitions = targetPartitions
      this
    }

    def withOptions(options: Map[String, String]): TableBuilder = {
      this.options = options
      this
    }

    def buildDSVTable(dfs: DFSWrapper, spark: SparkSession, external: Boolean): Table =
      buildTable(DSVFormat(Some(schema)), defaultDSVOptions ++ options, dfs, spark, external)

    def buildParquetTable(dfs: DFSWrapper, spark: SparkSession, external: Boolean): Table =
      buildTable(ParquetFormat(Some(schema)), options, dfs, spark, external)

    private def buildTable(
        format: DataFormat,
        options: Map[String, String],
        dfs: DFSWrapper,
        spark: SparkSession,
        external: Boolean
    ): Table = {
      createHiveTable(format, options, spark, external)
      new Table(dfs, spark, fullTableName, location, schema, targetPartitions, format, options)
    }

    private def createHiveTable(
        format: DataFormat,
        options: Map[String, String],
        spark: SparkSession,
        external: Boolean
    ): Unit = {
      val fieldMap = schema.fields.map(f => (f.name, f)).toMap
      val partitionColumnFields = targetPartitions.map(fieldMap)
      val columnFields = schema.fields.diff(partitionColumnFields)

      val partitionColumnDefinitions = Table.buildColumnDefinitions(partitionColumnFields)
      val columnDefinitions = Table.buildColumnDefinitions(columnFields)

      val statementBuilder = Array.newBuilder[String]

      if (external) statementBuilder += s"CREATE EXTERNAL TABLE $fullTableName($columnDefinitions)"
      else statementBuilder += s"CREATE TABLE $fullTableName($columnDefinitions)"

      if (targetPartitions.nonEmpty)
        statementBuilder += s"PARTITIONED BY ($partitionColumnDefinitions)"

      format match {
        case _: DataFormat.DSVFormat =>
          val delimiter = options("delimiter")
          statementBuilder += "ROW FORMAT DELIMITED"
          statementBuilder += s"FIELDS TERMINATED BY '$delimiter'"
        case _: DataFormat.ParquetFormat => statementBuilder += "STORED AS PARQUET"
        case anotherFormat               => throw new RuntimeException(s"Unknown file format: $anotherFormat")

      }

      statementBuilder += s"LOCATION '$location'"

      val statement = statementBuilder.result.mkString("\n")
      logger.info(s"Executing DDL statement:\n$statement")
      spark.sql(statement)
    }
  }
}
