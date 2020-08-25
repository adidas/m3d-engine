package com.adidas.analytics.config.loads

import com.adidas.analytics.algo.core.Algorithm.{SafeWriteOperation, UpdateStatisticsOperation}
import com.adidas.analytics.config.loads.DeltaLakeLoadConfiguration._
import com.adidas.analytics.config.shared.{
  ConfigurationContext,
  DateComponentDerivationConfiguration,
  MetadataUpdateStrategy
}
import com.adidas.analytics.util.DataFormat.ParquetFormat
import com.adidas.analytics.util.DataFrameUtils.PartitionCriteria
import com.adidas.analytics.util.{CatalogTableManager, LoadMode, OutputWriter}
import org.apache.spark.sql.catalyst.util.PermissiveMode
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import scala.util.parsing.json.JSONObject

trait DeltaLakeLoadConfiguration
    extends ConfigurationContext
    with LoadConfiguration
    with UpdateStatisticsOperation
    with MetadataUpdateStrategy
    with DateComponentDerivationConfiguration
    with SafeWriteOperation {

  protected def spark: SparkSession

  protected val isManualRepartitioning: Boolean =
    if (configReader.getAsOption[Int]("output_partitions_num").isEmpty) false else true

  protected val outputPartitionsNum: Option[Int] =
    configReader.getAsOption[Int]("output_partitions_num")

  configureDeltaSparkSession()

  // Delta Table Properties
  protected val deltaTableDir: String = configReader.getAs[String]("delta_table_dir")

  protected val businessKey: Seq[String] = configReader.getAsSeq[String]("business_key")

  protected val businessKeyMatchOperator: String =
    configReader.getAsOption[String]("business_key_match_operator").getOrElse("AND")
  protected val currentDataAlias = "currentData"
  protected val newDataAlias = "newData"

  protected val isToVacuum: Boolean =
    configReader.getAsOption[Boolean]("is_to_vacuum").getOrElse(true)

  protected val vacuumRetentionPeriod: Int =
    configReader.getAsOption[Int]("vacuum_retention_period").getOrElse(12)

  // Delta Condensation
  protected val condensationKey: Seq[String] = configReader.getAsSeq[String]("condensation_key")

  protected val recordModeColumnName: String = configReader.getAs[String]("record_mode_column")

  protected val recordsToCondense: Seq[String] =
    configReader.getAsSeq[String]("records_to_condense")

  protected val recordsToDelete: Seq[String] = configReader.getAsSeq[String]("records_to_delete")

  protected def recordModesFilterFunction: Row => Boolean = { row: Row =>
    recordsToCondense.contains(row.getAs[String](recordModeColumnName))
  }

  protected val initCondensationWithRecordMode: Boolean =
    configReader.getAsOption[Boolean]("init_condensation_with_record_mode").getOrElse(true)

  /* ignoreAffectedPartitionsMerge: covers the case where the partition key of the lake table can't
   * be used to control the merge match conditions, because it is not constant per record, meaning
   * it can change with time with the rest of the attributes of a record. It instructs the delta
   * lake load algorithm to ignore the delta table partitions when merging the current with new
   * data. */
  protected val ignoreAffectedPartitionsMerge: Boolean =
    configReader.getAsOption[Boolean]("ignore_affected_partitions_merge").getOrElse(true)

  protected var affectedPartitions: Seq[PartitionCriteria] = _

  // Target Properties
  protected val targetTable: String = configReader.getAs[String]("target_table")

  protected val targetSchema: StructType =
    CatalogTableManager(targetTable, spark).getSchemaSafely(dfs)

  protected val targetTableColumns: Array[String] = spark.table(targetTable).columns

  override protected val targetPartitions: Seq[String] =
    configReader.getAsSeq[String]("target_partitions")

  override protected val partitionSourceColumn: String =
    configReader.getAs[String]("partition_column")

  override protected val partitionSourceColumnFormat: String =
    configReader.getAs[String]("partition_column_format")

  override protected lazy val writer: OutputWriter.AtomicWriter = {
    if (targetPartitions.nonEmpty)
      OutputWriter.newTableLocationWriter(
        table = targetTable,
        format = ParquetFormat(Some(targetSchema)),
        targetPartitions = targetPartitions,
        metadataConfiguration = getMetaDataUpdateStrategy(targetTable, targetPartitions),
        loadMode = LoadMode.OverwritePartitions
      )
    else
      OutputWriter.newTableLocationWriter(
        table = targetTable,
        format = ParquetFormat(Some(targetSchema)),
        metadataConfiguration = getMetaDataUpdateStrategy(targetTable, targetPartitions),
        loadMode = LoadMode.OverwriteTable
      )
  }

  // JSON Source properties
  protected val isMultilineJSON: Option[Boolean] =
    configReader.getAsOption[Boolean]("is_multiline_json")

  protected val readJsonSchema: Option[StructType] =
    configReader.getAsOption[JSONObject]("schema") match {
      case Some(value) => Some(DataType.fromJson(value.toString()).asInstanceOf[StructType])
      case _           => None
    }

  override def loadMode: String = readerModeSetter(PermissiveMode.name)

  /** Configures the Spark session specially for the DeltaLakeLoad algorithm
    */
  def configureDeltaSparkSession(): Unit = {
    spark.conf.set(
      "spark.delta.logStore.class",
      "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore"
    )
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    spark.conf.set("spark.delta.merge.repartitionBeforeWrite", "true")
    spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    if (!isManualRepartitioning)
      spark.conf.set(
        "spark.sql.shuffle.partitions",
        Math.round(
          spark.conf.getOption("spark.executor.instances").getOrElse("100").toInt *
            spark.conf.getOption("spark.executor.cores").getOrElse("2").toInt
        )
      )

    logger.info(
      s"Auto schema merge is ${spark.conf.get("spark.databricks.delta.schema.autoMerge.enabled")}"
    )
    logger.info(
      s"Repartition before write is ${spark.conf.get("spark.delta.merge.repartitionBeforeWrite")}"
    )
    logger.info(s"Number of shuffle partitions: ${spark.conf.get("spark.sql.shuffle.partitions")}")
    logger.info(s"Spark serializer: ${spark.conf.get("spark.serializer")}")
  }

}

object DeltaLakeLoadConfiguration {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)
}
