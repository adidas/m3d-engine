package com.adidas.analytics.config.shared

import com.adidas.analytics.util.ConfigReader
import org.apache.spark.sql.catalyst.util.{DropMalformedMode}

trait LoadConfiguration {
  val STRUCTURED = "structured"
  val SEMISTRUCTURED = "semistructured"

  private val fileDelimiter: Option[String] = configReader.getAsOption[String]("delimiter")
  private val hasHeader: Option[Boolean] = configReader.getAsOption[Boolean]("has_header")

  private val optionalSparkOptions: Map[String, String] = Map[String, Option[String]](
    "nullValue" -> readNullValue,
    "quote" -> readQuoteValue
  ).collect {
    case (key, Some(value)) => (key, value)
  }

  private val requiredSparkOptions: Map[String, String] = Map[String, Option[Any]](
    "delimiter" -> fileDelimiter,
    "header" -> hasHeader,
    "mode" -> Some(loadMode)
  ).collect {
    case (key, Some(value)) => (key, value.toString)
  }


  protected val partitionColumns: Seq[String] = configReader.getAsSeq[String]("partition_columns")
  protected val inputDir: String = configReader.getAs[String]("source_dir")
  protected val fileFormat: String = configReader.getAs[String]("file_format")
  protected val dataType: String = configReader.getAsOption[String]("data_type").getOrElse(STRUCTURED)

  protected val sparkReaderOptions: Map[String, String] = requiredSparkOptions ++ optionalSparkOptions

  protected def configReader: ConfigReader
  protected def loadMode: String = DropMalformedMode.name
  protected def readNullValue: Option[String] = configReader.getAsOption[String]("null_value")
  protected def readQuoteValue: Option[String] = configReader.getAsOption[String]("quote_character")
  protected def computeTableStatistics: Boolean = configReader.getAsOption[Boolean]("compute_table_statistics").getOrElse(false)
}
