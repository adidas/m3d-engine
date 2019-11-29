package com.adidas.analytics.config

import com.adidas.analytics.config.shared.{ConfigurationContext, MetadataUpdateStrategy}
import com.adidas.analytics.algo.core.Algorithm.{ReadOperation, SafeWriteOperation}
import com.adidas.analytics.util.DataFormat.ParquetFormat
import com.adidas.analytics.util.{InputReader, LoadMode, OutputWriter}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType


trait NestedFlattenerConfiguration extends ConfigurationContext with ReadOperation with SafeWriteOperation with MetadataUpdateStrategy {

  protected def spark: SparkSession

  private val sourceLocation: String = configReader.getAs[String]("source_location")
  private val targetTable: String = configReader.getAs[String]("target_table")
  protected val targetPartitions: Option[Seq[String]] = configReader.getAsOptionSeq[String]("target_partitions")
  protected val targetSchema: StructType = spark.table(targetTable).schema
  protected val charsToReplace: String = configReader.getAs[String]("chars_to_replace")
  protected val replacementChar: String = configReader.getAs[String]("replacement_char")

  /*
   * Be aware of the naming pattern after flattening, because you also need to include sub-level structs or arrays if you want them.
   * Example: events__data if you want to flatten an array called "data" inside a struct called "events"
   */
  protected val fieldsToFlatten: Seq[String] = configReader.getAsSeq[String]("fields_to_flatten")

  /*
  * columnMapping provides the columns (with user-friendly names) to include in the final DataFrame.
  * Columns not in the nameMapping will be excluded
  */
  protected val columnMapping: Map[String, String] = configReader.getAsMap("column_mapping")

  override protected val readers: Vector[InputReader] = Vector(
    InputReader.newFileSystemReader(sourceLocation, ParquetFormat())
  )

  override protected val writer: OutputWriter.AtomicWriter = {
    var loadMode: LoadMode = LoadMode.OverwritePartitions
    if (targetPartitions.isEmpty)
      loadMode = LoadMode.OverwriteTable

    OutputWriter.newTableLocationWriter(
      targetTable,
      ParquetFormat(Some(targetSchema)),
      targetPartitions.getOrElse(Seq.empty),
      loadMode = loadMode,
      metadataConfiguration = getMetaDataUpdateStrategy(targetTable, targetPartitions.getOrElse(Seq.empty))
    )
  }

}
