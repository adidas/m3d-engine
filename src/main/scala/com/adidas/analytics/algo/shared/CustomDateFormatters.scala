package com.adidas.analytics.algo.shared

import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField

object CustomDateFormatters {

  /* Singletons of Custom Date Formatters */
  val YEAR_WEEK: DateTimeFormatter = new DateTimeFormatterBuilder()
    .appendValue(ChronoField.YEAR, 4)
    .appendValue(ChronoField.ALIGNED_WEEK_OF_YEAR, 2)
    .parseDefaulting(ChronoField.DAY_OF_WEEK, 1)
    .toFormatter()

  val YEAR_WEEK_DAY: DateTimeFormatter = new DateTimeFormatterBuilder()
    .appendValue(ChronoField.YEAR, 4)
    .appendValue(ChronoField.ALIGNED_WEEK_OF_YEAR, 2)
    .appendValue(ChronoField.DAY_OF_WEEK, 1)
    .toFormatter()

  val YEAR_MONTH: DateTimeFormatter = new DateTimeFormatterBuilder()
    .appendValue(ChronoField.YEAR, 4)
    .appendValue(ChronoField.MONTH_OF_YEAR, 2)
    .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
    .toFormatter()

  val MONTH_DAY_YEAR: DateTimeFormatter =
    new DateTimeFormatterBuilder()
      .appendValue(ChronoField.MONTH_OF_YEAR, 2)
      .appendLiteral("/")
      .appendValue(ChronoField.DAY_OF_MONTH, 2)
      .appendLiteral("/")
      .appendValue(ChronoField.YEAR, 4)
      .toFormatter

  val YEAR_MONTH_DAY_WITH_TIME: DateTimeFormatter =
    new DateTimeFormatterBuilder()
      .appendValue(ChronoField.YEAR, 4)
      .appendLiteral("-")
      .appendValue(ChronoField.MONTH_OF_YEAR, 2)
      .appendLiteral("-")
      .appendValue(ChronoField.DAY_OF_MONTH, 2)
      .appendLiteral(" ")
      .appendValue(ChronoField.HOUR_OF_DAY, 2)
      .appendLiteral(":")
      .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
      .appendLiteral(":")
      .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
      .toFormatter
}
