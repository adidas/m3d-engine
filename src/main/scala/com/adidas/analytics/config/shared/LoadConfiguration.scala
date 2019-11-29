package com.adidas.analytics.config.shared

import com.adidas.analytics.util.ConfigReader
import org.apache.spark.sql.catalyst.util.{DropMalformedMode, FailFastMode, PermissiveMode}

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


  protected val targetPartitions: Seq[String] = configReader.getAsSeq[String]("target_partitions")
  protected val inputDir: String = configReader.getAs[String]("source_dir")
  protected val fileFormat: String = configReader.getAs[String]("file_format")
  protected val dataType: String = configReader.getAsOption[String]("data_type").getOrElse(STRUCTURED)


  protected val sparkReaderOptions: Map[String, String] = requiredSparkOptions ++ optionalSparkOptions

  protected def configReader: ConfigReader

  protected def loadMode: String

  protected def readNullValue: Option[String] = configReader.getAsOption[String]("null_value")

  protected def readQuoteValue: Option[String] = configReader.getAsOption[String]("quote_character")

  protected def computeTableStatistics: Boolean = configReader.getAsOption[Boolean]("compute_table_statistics").getOrElse(false)

  protected def readerModeSetter(defaultMode: String): String = {
    configReader.getAsOption[String]("reader_mode") match {
      case Some(mode) => {
        mode.toUpperCase match {
          case PermissiveMode.name => PermissiveMode.name
          case FailFastMode.name => FailFastMode.name
          case DropMalformedMode.name => DropMalformedMode.name
          case _ => throw new RuntimeException(s"Invalid reader mode: $mode provided")
        }
      }
      case None => defaultMode
    }
  }
}
