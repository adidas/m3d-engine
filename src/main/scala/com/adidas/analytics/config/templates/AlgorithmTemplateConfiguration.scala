package com.adidas.analytics.config.templates

import com.adidas.analytics.algo.core.Algorithm.{
  ReadOperation,
  SafeWriteOperation,
  UpdateStatisticsOperation
}
import com.adidas.analytics.config.shared.{ConfigurationContext, MetadataUpdateStrategy}
import com.adidas.analytics.config.templates.AlgorithmTemplateConfiguration.ruleToLocalDate
import com.adidas.analytics.util.DataFormat.ParquetFormat
import com.adidas.analytics.util.{CatalogTableManager, InputReader, LoadMode, OutputWriter}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.joda.time.format.DateTimeFormat
import org.joda.time.{Days, LocalDate}

trait AlgorithmTemplateConfiguration
    extends ConfigurationContext
    with ReadOperation
    with SafeWriteOperation
    with UpdateStatisticsOperation
    with MetadataUpdateStrategy {

  protected def spark: SparkSession

  /** This trait has the responsibility to obtain the required configurations for a given algorithm.
    * In this template, it can be seen that values like source and target tables, dates, and readers
    * and writers, are obtained in this class by mixing ConfigurationContext, ReadOperation and
    * SafeWriteOperation.
    *
    * At the same time, AlgorithmTemplateConfiguration is mixed in the AlgorithmTemplate class, so
    * it can use the values from the provided configuration.
    *
    * An easy way to see this, is to think of it as the parser of the algorithm json config file.
    */
  protected val sourceTable: String = configReader.getAs[String]("source_table").trim
  /* you can use a source location as parquet files on the lake instead of a hive table */
  /* protected val sourceLocation: String =
   * configReader.getAs[String]("source_location").trim */

  protected val targetTable: String = configReader.getAs[String]("target_table").trim

  protected val startDate: LocalDate = ruleToLocalDate(configReader.getAs[String]("date_from").trim)

  protected val endDate: LocalDate = ruleToLocalDate(configReader.getAs[String]("date_to").trim)

  protected val dateRange: Days = Days.daysBetween(startDate, endDate)

  protected val targetSchema: StructType =
    CatalogTableManager(targetTable, spark).getSchemaSafely(dfs)

  override protected val readers: Vector[InputReader.TableReader] = Vector(
    // Obtaining a reader for the algorithm.
    InputReader.newTableReader(table = sourceTable)
    /* you can use a source location as parquet files on the lake instead of a hive table */
    /* InputReader.newFileSystemReader(sourceLocation, DataFormat.ParquetFormat()) */
  )

  override protected val writer: OutputWriter.AtomicWriter =
    /** Obtaining a writer for the algorithm.
      *
      * Note that the LoadMode can be any of the following:
      *
      * -- OverwriteTable: which steps on the exiting files and writes the new records
      * -- OverwritePartitions: which steps on the existing files inside a partition directory
      * -- AppendJoinPartitions: which appends the records to existing ones in the partition
      * directory by a Full Outer Join
      * -- AppendUnionPartition: which appends the records to existing ones in the partition
      * directory by a Union All
      */
    OutputWriter.newTableLocationWriter(
      table = targetTable,
      format = ParquetFormat(Some(targetSchema)),
      metadataConfiguration = getMetaDataUpdateStrategy(targetTable, Seq("", "", "")),
      targetPartitions = Seq("", "", ""),
      /* If partitions are required, this would look like, e.g., Seq("year", "month") */
      loadMode = LoadMode.OverwritePartitionsWithAddedColumns
    )
}

object AlgorithmTemplateConfiguration {

  /** A companion object can alternatively be used to add helper methods In this case, there is a
    * method to convert a date string to a specific date value, because in this example, date could
    * also contain a string such as today and yesterday, as well as a pattern.
    */

  private val DatePattern = "([0-9]{4}-[0-9]{2}-[0-9]{2})".r
  private val DateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")

  private def ruleToLocalDate(rule: String): LocalDate =
    rule.trim match {
      case DatePattern(dateString) => LocalDate.parse(dateString, DateFormatter)
      case "today"                 => LocalDate.now()
      case "yesterday"             => LocalDate.now().minus(Days.ONE)
      case _                       => throw new IllegalArgumentException(s"Invalid date format: $rule")
    }
}
