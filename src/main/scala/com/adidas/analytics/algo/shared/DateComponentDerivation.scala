package com.adidas.analytics.algo.shared

import java.time.LocalDate
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.types.{IntegerType, StringType}

import scala.util.{Failure, Success, Try}


trait DateComponentDerivation {

  protected val tempFormatterColumnName: String = "temp_formatter_column"

  protected def withDateComponents(sourceDateColumnName: String, sourceDateFormat: String, targetDateComponentColumnNames: Seq[String])(inputDf: DataFrame): DataFrame = {
    targetDateComponentColumnNames.foldLeft(inputDf.withColumn(tempFormatterColumnName, lit(sourceDateFormat))) {
      (df, colName) =>
        colName match {
          case "year" =>
            withDateComponent(df, sourceDateColumnName, colName, 9999, customYear)
          case "month" =>
            withDateComponent(df, sourceDateColumnName, colName, 99, customMonth)
          case "day" =>
            withDateComponent(df, sourceDateColumnName, colName, 99, customDay)
          case "week" =>
            withDateComponent(df, sourceDateColumnName, colName, 99, customWeekOfYear)
          case everythingElse =>
            throw new RuntimeException(s"Unable to infer a partitioning column for: $everythingElse.")
        }
    }.drop(tempFormatterColumnName)
  }

  private def withDateComponent(inputDf: DataFrame,
                                   sourceDateColumnName: String,
                                   targetColumnName: String,
                                   defaultValue: Int,
                                   derivationFunction: UserDefinedFunction): DataFrame = {

    inputDf
      .withColumn(targetColumnName,
        when(
          derivationFunction(col(sourceDateColumnName).cast(StringType), col(tempFormatterColumnName)).isNotNull,
          derivationFunction(col(sourceDateColumnName).cast(StringType), col(tempFormatterColumnName)))
          .otherwise(lit(defaultValue))
      )
  }

  private val customWeekOfYear = udf((ts: String, formatter: String) => {
      Try {
        getCustomFormatter(formatter) match {
          case Some(customFormatter) =>
            LocalDate.parse(ts, customFormatter).get(ChronoField.ALIGNED_WEEK_OF_YEAR)
          case None => None
        }
      } match {
        case Failure(_) => None
        case Success(value) => value
      }
  }, IntegerType)

  private val customYear = udf((ts: String, formatter: String) => {
    Try {
      getCustomFormatter(formatter) match {
        case Some(customFormatter) =>
          LocalDate.parse(ts, customFormatter).get(ChronoField.YEAR)
        case None =>
          LocalDate.parse(ts, DateTimeFormatter.ofPattern(formatter)).getYear
      }
    } match {
      case Failure(_) => None
      case Success(value) => value
    }
  }, IntegerType)

  private val customDay = udf((ts: String, formatter: String) => {
    Try {
      getCustomFormatter(formatter) match {
        // note: this logic must be updated if we have
        // customFormatters with dayOfMonth
        case Some(customFormatter) =>
          LocalDate.parse(ts, customFormatter).get(ChronoField.DAY_OF_WEEK)
        case None =>
          LocalDate.parse(ts, DateTimeFormatter.ofPattern(formatter)).getDayOfMonth
      }
    } match {
      case Failure(_) => None
      case Success(value) => value
    }
  }, IntegerType)

  private val customMonth = udf((ts: String, formatter: String) => {
    Try {
      getCustomFormatter(formatter) match {
        case Some(customFormatter) =>
          LocalDate.parse(ts, customFormatter).getMonthValue
        case None =>
          LocalDate.parse(ts, DateTimeFormatter.ofPattern(formatter)).getMonthValue
      }
    } match {
      case Failure(_) => None
      case Success(value) => value
    }
  }, IntegerType)

  private def getCustomFormatter(dateFormatter: String): Option[DateTimeFormatter] =
    dateFormatter match {
      case "yyyyww" => Option(CustomDateFormatters.YEAR_WEEK)
      case "yyyywwe" => Option(CustomDateFormatters.YEAR_WEEK_DAY)
      case "yyyyMM" => Option(CustomDateFormatters.YEAR_MONTH)
      case _ => None
    }

}
