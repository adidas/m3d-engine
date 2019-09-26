package com.adidas.analytics.config.shared

import com.adidas.analytics.util.ConfigReader
import org.apache.spark.sql.catalyst.util.{DropMalformedMode}

trait LoadConfiguration {

  private val fileDelimiter: String = configReader.getAs[String]("delimiter")
  private val hasHeader: Boolean = configReader.getAs[Boolean]("has_header")

  private val optionalSparkOptions: Map[String, String] = Map[String, Option[String]](
    "nullValue" -> readNullValue,
    "quote" -> readQuoteValue
  ).collect {
    case (key, Some(value)) => (key, value)
  }

  private val requiredSparkOptions: Map[String, String] = Map[String, String](
    "delimiter" -> fileDelimiter,
    "header" -> hasHeader.toString,
    "mode" -> loadMode
  )

  protected val partitionColumns: Seq[String] = configReader.getAsSeq[String]("partition_columns")
  protected val inputDir: String = configReader.getAs[String]("source_dir")
  protected val targetTable: String = configReader.getAs[String]("target_table")
  protected val fileFormat: String = configReader.getAs[String]("file_format")

  protected val sparkReaderOptions: Map[String, String] = requiredSparkOptions ++ optionalSparkOptions

  protected def configReader: ConfigReader
  protected def loadMode: String = DropMalformedMode.name
  protected def readNullValue: Option[String] = configReader.getAsOption[String]("null_value")
  protected def readQuoteValue: Option[String] = configReader.getAsOption[String]("quote_character")
  protected def computeTableStatistics: Boolean = configReader.getAsOption[Boolean]("compute_table_statistics").getOrElse(false)
}
