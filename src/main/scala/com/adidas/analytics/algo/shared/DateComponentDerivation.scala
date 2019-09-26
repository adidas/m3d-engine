package com.adidas.analytics.algo.shared

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame}


trait DateComponentDerivation {

  private val TempTimeStampColumnName: String = "__temp_timestamp_column__"

  protected def withDateComponents(sourceDateColumnName: String, sourceDateFormat: String, targetDateComponentColumnNames: Seq[String])(inputDf: DataFrame): DataFrame = {
    targetDateComponentColumnNames.foldLeft(inputDf.withColumn(TempTimeStampColumnName, to_date(col(sourceDateColumnName).cast(StringType), sourceDateFormat))) {
      (df, colName) =>
        colName match {
          case "year" => withDateComponent(df, "year", 9999, year)
          case "month" => withDateComponent(df, "month", 99, month)
          case "day" => withDateComponent(df, "day", 99, dayofmonth)
          case "week" => withDateComponent(df, "week", 99, weekofyear)
          case everythingElse => throw new RuntimeException(s"Unable to infer a partitioning column for: $everythingElse.")
        }
    }.drop(TempTimeStampColumnName)
  }

  private def withDateComponent(inputDf: DataFrame, targetColumnName: String, defaultValue: Int, derivationFunction: Column => Column): DataFrame = {
    inputDf.withColumn(targetColumnName, when(derivationFunction(col(TempTimeStampColumnName)).isNotNull, derivationFunction(col(TempTimeStampColumnName))).otherwise(lit(defaultValue)))
  }
}
