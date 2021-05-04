package com.adidas.analytics.algo.shared

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

import scala.util.{Failure, Success, Try}

trait DateComponentDerivation {

  protected val tempFormatterColumnName: String = "temp_formatter_column"

  protected def withDateComponents(
      sourceDateColumnName: String,
      sourceDateFormat: String,
      targetDateComponentColumnNames: Seq[String]
  )(inputDf: DataFrame): DataFrame =
    targetDateComponentColumnNames
      .foldLeft(
        inputDf.withColumn(tempFormatterColumnName, lit(sourceDateFormat))
      ) { (df, colName) =>
        if (DateComponentDerivation.ALLOWED_DERIVATIONS.contains(colName)) colName match {
          case "year" =>
            withDateComponent(
              df,
              sourceDateColumnName,
              colName,
              customYear
            )
          case "month" =>
            withDateComponent(
              df,
              sourceDateColumnName,
              colName,
              customMonth
            )
          case "day" =>
            withDateComponent(
              df,
              sourceDateColumnName,
              colName,
              customDay
            )
          case "week" =>
            withDateComponent(
              df,
              sourceDateColumnName,
              colName,
              customWeekOfYear
            )
        }
        else
          throw new RuntimeException(
            s"Unable to derive a partitioning column for $colName as the same does not " +
              s"belong to the set of allowed derivations."
          )
      }
      .drop(tempFormatterColumnName)

  private def withDateComponent(
      inputDf: DataFrame,
      sourceDateColumnName: String,
      targetColumnName: String,
      derivationFunction: UserDefinedFunction
  ): DataFrame =
    inputDf.withColumn(
      targetColumnName,
      derivationFunction(col(sourceDateColumnName).cast(StringType), col(tempFormatterColumnName))
    )

  private val customWeekOfYear: UserDefinedFunction = udf { (ts: String, formatter: String) =>
    Try {
      getCustomFormatter(formatter) match {
        case Some(customFormatter) =>
          LocalDate
            .parse(ts, customFormatter)
            .get(ChronoField.ALIGNED_WEEK_OF_YEAR)
        case None =>
          LocalDate
            .parse(ts, DateTimeFormatter.ofPattern(formatter))
            .get(ChronoField.ALIGNED_WEEK_OF_YEAR)
      }
    } match {
      case Failure(_)     => DateComponentDerivation.DEFAULT_2DIGIT_VALUE
      case Success(value) => value
    }
  }

  private val customYear = udf((ts: String, formatter: String) =>
    Try {
      getCustomFormatter(formatter) match {
        case Some(customFormatter) => LocalDate.parse(ts, customFormatter).get(ChronoField.YEAR)
        case None                  => LocalDate.parse(ts, DateTimeFormatter.ofPattern(formatter)).getYear
      }
    } match {
      case Failure(_)     => DateComponentDerivation.DEFAULT_4DIGIT_VALUE
      case Success(value) => value
    }
  )

  private val customDay = udf((ts: String, formatter: String) =>
    Try {
      getCustomFormatter(formatter) match {
        case Some(customFormatter) =>
          val dayType =
            if (formatter.contains("dd")) ChronoField.DAY_OF_MONTH else ChronoField.DAY_OF_WEEK
          LocalDate.parse(ts, customFormatter).get(dayType)
        case None => LocalDate.parse(ts, DateTimeFormatter.ofPattern(formatter)).getDayOfMonth
      }
    } match {
      case Failure(_)     => DateComponentDerivation.DEFAULT_2DIGIT_VALUE
      case Success(value) => value
    }
  )

  private val customMonth = udf((ts: String, formatter: String) =>
    Try {
      getCustomFormatter(formatter) match {
        case Some(customFormatter) => LocalDate.parse(ts, customFormatter).getMonthValue
        case None                  => LocalDate.parse(ts, DateTimeFormatter.ofPattern(formatter)).getMonthValue
      }
    } match {
      case Failure(_)     => DateComponentDerivation.DEFAULT_2DIGIT_VALUE
      case Success(value) => value
    }
  )

  private def getCustomFormatter(dateFormatter: String): Option[DateTimeFormatter] =
    dateFormatter match {
      case "yyyyww"              => Option(CustomDateFormatters.YEAR_WEEK)
      case "yyyywwe"             => Option(CustomDateFormatters.YEAR_WEEK_DAY)
      case "yyyyMM"              => Option(CustomDateFormatters.YEAR_MONTH)
      case "MM/dd/yyyy"          => Option(CustomDateFormatters.MONTH_DAY_YEAR)
      case "yyyy-MM-dd HH:mm:ss" => Option(CustomDateFormatters.YEAR_MONTH_DAY_WITH_TIME)
      case _                     => None
    }

}

object DateComponentDerivation {

  val ALLOWED_DERIVATIONS: Seq[String] = Seq[String]("year", "month", "day", "week")
  val DEFAULT_4DIGIT_VALUE = 9999
  val DEFAULT_2DIGIT_VALUE = 99
}
