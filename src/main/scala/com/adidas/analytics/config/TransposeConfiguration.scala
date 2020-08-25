package com.adidas.analytics.config

import com.adidas.analytics.algo.core.Algorithm.{
  ReadOperation,
  SafeWriteOperation,
  UpdateStatisticsOperation
}
import com.adidas.analytics.config.shared.{ConfigurationContext, MetadataUpdateStrategy}
import com.adidas.analytics.util.DataFormat.ParquetFormat
import com.adidas.analytics.util.{CatalogTableManager, InputReader, LoadMode, OutputWriter}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

trait TransposeConfiguration
    extends ConfigurationContext
    with ReadOperation
    with SafeWriteOperation
    with UpdateStatisticsOperation
    with MetadataUpdateStrategy {

  protected def spark: SparkSession
  protected val sourceTable: String = configReader.getAs[String]("source_table")
  protected val targetTable: String = configReader.getAs[String]("target_table")

  protected val targetPartitions: Option[Seq[String]] =
    configReader.getAsOptionSeq[String]("target_partitions")

  protected val targetSchema: StructType =
    CatalogTableManager(targetTable, spark).getSchemaSafely(dfs)

  protected val aggregationColumn: String = configReader.getAs[String]("aggregation_column")
  protected val pivotColumn: String = configReader.getAs[String]("pivot_column")

  protected val groupByColumn: Seq[String] = configReader.getAsSeq[String]("group_by_column")

  protected val enforceSchema: Boolean =
    configReader.getAsOption[Boolean]("enforce_schema").getOrElse(false)

  override protected val readers: Vector[InputReader] =
    Vector(InputReader.newTableReader(sourceTable))

  override protected val writer: OutputWriter.AtomicWriter = {
    var loadMode: LoadMode = LoadMode.OverwritePartitions
    if (targetPartitions.isEmpty) loadMode = LoadMode.OverwriteTable

    OutputWriter.newTableLocationWriter(
      targetTable,
      ParquetFormat(Some(targetSchema)),
      targetPartitions.getOrElse(Seq.empty),
      loadMode = loadMode,
      metadataConfiguration =
        getMetaDataUpdateStrategy(targetTable, targetPartitions.getOrElse(Seq.empty))
    )
  }
}
