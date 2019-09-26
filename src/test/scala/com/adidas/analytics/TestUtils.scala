package com.adidas.analytics

import org.apache.spark.sql.functions.{col, count, lit}
import org.apache.spark.sql.{DataFrame, Row}

object TestUtils {

  implicit class ExtendedDataFrame(df: DataFrame) {

    def hasDiff(anotherDf: DataFrame): Boolean = {
      def printDiff(incoming: Boolean)(row: Row): Unit = {
        if (incoming) print("+ ") else print("- ")
        println(row)
      }

      val groupedDf = df.groupBy(df.columns.map(col): _*).agg(count(lit(1))).collect().toSet
      val groupedAnotherDf = anotherDf.groupBy(anotherDf.columns.map(col): _*).agg(count(lit(1))).collect().toSet

      groupedDf.diff(groupedAnotherDf).foreach(printDiff(incoming = true))
      groupedAnotherDf.diff(groupedDf).foreach(printDiff(incoming = false))

      groupedDf.diff(groupedAnotherDf).nonEmpty || groupedAnotherDf.diff(groupedDf).nonEmpty
    }
  }
}
