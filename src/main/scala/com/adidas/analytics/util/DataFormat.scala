package com.adidas.analytics.util

import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}


sealed trait DataFormat {

  protected val logger: Logger = LoggerFactory.getLogger(getClass)

  def read(reader: DataFrameReader, locations: String*): DataFrame

  def write(writer: DataFrameWriter[Row], location: String): Unit
}


object DataFormat {

  case class ParquetFormat(optionalSchema: Option[StructType] = None) extends DataFormat {

    override def read(reader: DataFrameReader, locations: String*): DataFrame = {
      val filesString = locations.mkString(", ")
      logger.info(s"Reading Parquet data from $filesString")
      optionalSchema.fold(reader)(schema => reader.schema(schema)).parquet(locations: _*)
    }

    override def write(writer: DataFrameWriter[Row], location: String): Unit = {
      logger.info(s"Writing Parquet data to $location")
      writer.parquet(location)
    }
  }

  case class DSVFormat(optionalSchema: Option[StructType] = None) extends DataFormat {

    override def read(reader: DataFrameReader, locations: String*): DataFrame = {
      val filesString = locations.mkString(", ")
      logger.info(s"Reading DSV data from $filesString")
      optionalSchema.fold(reader.option("inferSchema", "true"))(schema => reader.schema(schema)).csv(locations: _*)
    }

    override def write(writer: DataFrameWriter[Row], location: String): Unit = {
      logger.info(s"Writing DSV data to $location")
      writer.csv(location)
    }
  }
}

