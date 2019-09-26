package com.adidas.analytics.util

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Base trait for classes which are capable of reading data into DataFrames
  */
sealed abstract class InputReader {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  def read(sparkSession: SparkSession): DataFrame
}


object InputReader {

  /**
    * Factory method which creates TableReader
    *
    * @param table source table to read data from
    * @param options Spark reader options
    * @return TableReader
    */
  def newTableReader(table: String, options: Map[String, String] = Map.empty): TableReader = {
    TableReader(table, options)
  }

  /**
    * Factory method which creates FileSystemReader
    *
    * @param location location to read data from
    * @param format format of source data
    * @param options Spark reader options
    * @return FileSystemReader
    */
  def newFileSystemReader(location: String, format: DataFormat, options: Map[String, String] = Map.empty): FileSystemReader = {
    FileSystemReader(location, format, options)
  }

  /**
    * Factory method which creates TableLocationReader
    *
    * @param table source table which location is used to read data from
    * @param format format of source data
    * @param options Spark reader options
    * @return TableLocationReader
    */
  def newTableLocationReader(table: String, format: DataFormat, options: Map[String, String] = Map.empty): TableLocationReader = {
    TableLocationReader(table, format, options)
  }

  case class TableReader(table: String, options: Map[String, String]) extends InputReader {
    override def read(sparkSession: SparkSession): DataFrame = {
      logger.info(s"Reading data from table $table")
      sparkSession.read.options(options).table(table)
    }
  }

  case class FileSystemReader(location: String, format: DataFormat, options: Map[String, String]) extends InputReader {
    override def read(sparkSession: SparkSession): DataFrame = {
      logger.info(s"Reading data from location $location")
      format.read(sparkSession.read.options(options), location)
    }
  }

  case class TableLocationReader(table: String, format: DataFormat, options: Map[String, String]) extends InputReader {
    override def read(sparkSession: SparkSession): DataFrame = {
      val location = HiveTableAttributeReader(table, sparkSession).getTableLocation
      logger.info(s"Reading data from location $location")
      format.read(sparkSession.read.options(options), location)
    }
  }
}
