package com.adidas.analytics.util

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


class HiveTableAttributeReader(table: String, sparkSession: SparkSession) {
  private val KeyColumn = col("col_name")
  private val ValueColumn = col("data_type")
  private val LocationKey = "LOCATION"


  private lazy val attributes: Map[String, String] = sparkSession.sql(s"describe formatted $table")
    .select(KeyColumn, ValueColumn)
    .withColumn(KeyColumn.toString, upper(KeyColumn))
    .collect()
    .map(row => (row.getAs[String](KeyColumn.toString), row.getAs[String](ValueColumn.toString)))
    .toMap

  /**
    * Generic method to read attribute values from hive table description output
    * @param key Key to be read (First column)
    * @return Value (Second Column)
    */
  def getValueForKey(key: String): String = attributes(key.toUpperCase)

  /**
    * Read table location
    * @return table location
    */
  def getTableLocation: String = getValueForKey(LocationKey)
}


object HiveTableAttributeReader {
  def apply(tableName: String, sparkSession: SparkSession): HiveTableAttributeReader = {
    new HiveTableAttributeReader(tableName, sparkSession)
  }
}
