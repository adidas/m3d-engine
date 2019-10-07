package com.adidas.analytics.config

import com.adidas.analytics.algo.core.Algorithm.{ReadOperation, SafeWriteOperation}
import com.adidas.analytics.config.FixedSizeStringExtractorConfiguration._
import com.adidas.analytics.config.shared.ConfigurationContext
import com.adidas.analytics.util.DataFormat.ParquetFormat
import com.adidas.analytics.util.DataFrameUtils.PartitionCriteria
import com.adidas.analytics.util.{InputReader, LoadMode, OutputWriter}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.joda.time._
import org.slf4j.{Logger, LoggerFactory}


trait FixedSizeStringExtractorConfiguration extends ConfigurationContext with ReadOperation with SafeWriteOperation {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  protected def spark: SparkSession

  private val sourceTable: String = configReader.getAs[String]("source_table").trim
  private val targetTable: String = configReader.getAs[String]("target_table").trim

  protected val sourceField: String = configReader.getAs[String]("source_field").trim
  protected val partitionColumnsOrdered: Seq[String] = configReader.getAsSeq[String]("partition_columns")
  protected val partitionColumnsSet: Set[String] = partitionColumnsOrdered.toSet

  protected val partitionsCriteria: PartitionCriteria = {
    if (configReader.contains("select_conditions")) {
      if (partitionColumnsOrdered.nonEmpty) {
        parseConditions(configReader.getAsSeq[String]("select_conditions"))
      } else {
        logger.warn("Select conditions can be applied to partitioned tables only. Ignoring.")
        Seq.empty
      }
    } else if (configReader.contains("select_rules")) {
      if (partitionColumnsOrdered.nonEmpty) {
        parseRules(configReader.getAsSeq[String]("select_rules"), partitionColumnsOrdered, partitionColumnsSet)
      } else {
        logger.warn("Select rules can be applied to partitioned tables only. Ignoring.")
        Seq.empty
      }
    } else {
      Seq.empty
    }
  }

  protected val substringPositions: Seq[(Int, Int)] = configReader.getAsSeq[String]("substring_positions").map {
    case NumberPairPattern(start, end) => (start.toInt, end.toInt)
    case another => throw new IllegalArgumentException(s"Wrong select condition: $another")
  }

  protected val targetSchema: StructType = spark.table(targetTable).schema

  override protected val readers: Vector[InputReader] = Vector(
    InputReader.newTableReader(table = sourceTable)
  )

  override protected val writer: OutputWriter.AtomicWriter = OutputWriter.newTableLocationWriter(
    table = targetTable,
    format = ParquetFormat(Some(targetSchema)),
    partitionColumns = partitionColumnsOrdered,
    loadMode = if (partitionColumnsOrdered.nonEmpty) LoadMode.OverwritePartitionsWithAddedColumns else LoadMode.OverwriteTable
  )
}


object FixedSizeStringExtractorConfiguration {

  private val Year = "year"
  private val Month = "month"
  private val Week = "week"
  private val Day = "day"

  private val RulePattern = s"($Year|$Month|$Week|$Day)([+-])([0-9]+)".r
  private val ConditionPattern = "(.+?)[ ]*=[ ]*(.+)".r
  private val NumberPairPattern = "([0-9]+?)[ ]*,[ ]*([0-9]+)".r

  private def parseConditions(conditions: Seq[String]): PartitionCriteria = {
    conditions.map {
      case ConditionPattern(columnName, columnValue) => (columnName.trim, columnValue.trim)
      case condition => throw new IllegalArgumentException(s"Wrong select condition: $condition")
    }
  }

  private def parseRules(rules: Seq[String], partitionColumnsOrdered: Seq[String], partitionColumnsSet: Set[String]): PartitionCriteria = {
    if (rules.nonEmpty) {
      val selectDate = rules.foldLeft(LocalDate.now()) {
        case (date, RulePattern(period, "-", value)) =>
          if (!partitionColumnsSet.contains(period)) {
            throw new RuntimeException(s"Unsupported period: $period")
          }
          date.minus(createPeriodByNameAndValue(period, value.toInt))
        case (date, RulePattern(period, "+", value)) =>
          if (!partitionColumnsSet.contains(period)) {
            throw new RuntimeException(s"Unsupported period: $period")
          }
          date.plus(createPeriodByNameAndValue(period, value.toInt))
        case rule =>
          throw new IllegalArgumentException(s"Wrong select rule: $rule")
      }
      createCriteriaForDate(selectDate, partitionColumnsOrdered)
    } else {
      Seq.empty
    }
  }

  private def createCriteriaForDate(date: LocalDate, partitionColumns: Seq[String]): PartitionCriteria = {
    partitionColumns match {
      case Year :: Month :: Day :: Nil =>
        Seq(Year -> date.getYear.toString, Month -> date.getMonthOfYear.toString, Day -> date.getDayOfMonth.toString)
      case Year :: Month :: Nil =>
        Seq(Year -> date.getYear.toString, Month -> date.getMonthOfYear.toString)
      case Year :: Week :: Nil =>
        Seq(Year -> date.getYear.toString, Week -> date.getWeekOfWeekyear.toString)
      case _ => throw new RuntimeException(s"Unsupported partitioning schema: $partitionColumns")
    }
  }

  private def createPeriodByNameAndValue(name: String, value: Int): ReadablePeriod = {
    name match {
      case Year => Years.years(value)
      case Month => Months.months(value)
      case Week => Weeks.weeks(value)
      case Day => Days.days(value)
    }
  }
}
