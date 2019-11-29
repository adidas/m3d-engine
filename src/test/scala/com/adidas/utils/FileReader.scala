package com.adidas.utils

import com.adidas.analytics.util.DataFormat
import com.adidas.analytics.util.DataFormat.{DSVFormat, JSONFormat, ParquetFormat}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}


class FileReader(format: DataFormat, options: Map[String, String]) {

  def read(spark: SparkSession, location: String, fillNulls: Boolean = false): DataFrame = {
    val df = format.read(spark.read.options(options), location)
    if (fillNulls) {
      df.na.fill("")
    } else {
      df
    }
  }
}


object FileReader {

  def newDSVFileReader(optionalSchema: Option[StructType] = None, delimiter: Char = '|', header: Boolean = false): FileReader = {
    val options = Map("delimiter" -> delimiter.toString, "header" -> header.toString)
    if (optionalSchema.isEmpty) {
      new FileReader(DSVFormat(optionalSchema), options + ("inferSchema" -> "true"))
    } else {
      new FileReader(DSVFormat(optionalSchema), options)
    }
  }

  def newParquetFileReader(): FileReader = {
    new FileReader(ParquetFormat(), Map.empty[String, String])
  }

  def newJsonFileReader(optionalSchema: Option[StructType] = None): FileReader = {
      new FileReader(JSONFormat(optionalSchema), Map.empty[String, String])
  }

  def apply(format: DataFormat, options: (String, String)*): FileReader = {
    new FileReader(format, options.toMap)
  }

  def apply(format: DataFormat, options: Map[String, String]): FileReader = {
    new FileReader(format, options)
  }
}
