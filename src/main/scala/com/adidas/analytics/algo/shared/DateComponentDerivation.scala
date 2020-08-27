package com.adidas.analytics.algo.shared

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{udf, _}
import org.apache.spark.sql.types.{IntegerType, StringType}
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
              DateComponentDerivation.DEFAULT_4DIGIT_VALUE,
              customYear
            )
          case "month" =>
            withDateComponent(
              df,
              sourceDateColumnName,
              colName,
              DateComponentDerivation.DEFAULT_2DIGIT_VALUE,
              customMonth
            )
          case "day" =>
            withDateComponent(
              df,
              sourceDateColumnName,
              colName,
              DateComponentDerivation.DEFAULT_2DIGIT_VALUE,
              customDay
            )
          case "week" =>
            withDateComponent(
              df,
              sourceDateColumnName,
              colName,
              DateComponentDerivation.DEFAULT_2DIGIT_VALUE,
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
      defaultValue: Int,
      derivationFunction: UserDefinedFunction
  ): DataFrame =
    inputDf.withColumn(
      targetColumnName,
      when(
        derivationFunction(
          col(sourceDateColumnName).cast(StringType),
          col(tempFormatterColumnName)
        ).isNotNull,
        derivationFunction(col(sourceDateColumnName).cast(StringType), col(tempFormatterColumnName))
      ).otherwise(lit(defaultValue))
    )

  private val customWeekOfYear = udf(
    (ts: String, formatter: String) =>
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
        case Failure(_)     => None
        case Success(value) => value
      },
    IntegerType
  )

  private val customYear = udf(
    (ts: String, formatter: String) =>
      Try {
        getCustomFormatter(formatter) match {
          case Some(customFormatter) => LocalDate.parse(ts, customFormatter).get(ChronoField.YEAR)
          case None                  => LocalDate.parse(ts, DateTimeFormatter.ofPattern(formatter)).getYear
        }
      } match {
        case Failure(_)     => None
        case Success(value) => value
      },
    IntegerType
  )

  private val customDay = udf(
    (ts: String, formatter: String) =>
      Try {
        getCustomFormatter(formatter) match {
          case Some(customFormatter) =>
            val day_type =
              if (formatter.contains("dd")) ChronoField.DAY_OF_MONTH else ChronoField.DAY_OF_WEEK
            LocalDate.parse(ts, customFormatter).get(day_type)
          case None => LocalDate.parse(ts, DateTimeFormatter.ofPattern(formatter)).getDayOfMonth
        }
      } match {
        case Failure(_)     => None
        case Success(value) => value
      },
    IntegerType
  )

  private val customMonth = udf(
    (ts: String, formatter: String) =>
      Try {
        getCustomFormatter(formatter) match {
          case Some(customFormatter) => LocalDate.parse(ts, customFormatter).getMonthValue
          case None                  => LocalDate.parse(ts, DateTimeFormatter.ofPattern(formatter)).getMonthValue
        }
      } match {
        case Failure(_)     => None
        case Success(value) => value
      },
    IntegerType
  )

  private def getCustomFormatter(dateFormatter: String): Option[DateTimeFormatter] =
    dateFormatter match {
      case "yyyyww"     => Option(CustomDateFormatters.YEAR_WEEK)
      case "yyyywwe"    => Option(CustomDateFormatters.YEAR_WEEK_DAY)
      case "yyyyMM"     => Option(CustomDateFormatters.YEAR_MONTH)
      case "MM/dd/yyyy" => Option(CustomDateFormatters.MONTH_DAY_YEAR)
      case _            => None
    }

}

object DateComponentDerivation {

  val ALLOWED_DERIVATIONS: Seq[String] = Seq[String]("year", "month", "day", "week")
  val DEFAULT_4DIGIT_VALUE = 9999
  val DEFAULT_2DIGIT_VALUE = 99
}
