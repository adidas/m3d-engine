package com.adidas.analytics.util

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, functions}
import org.slf4j.{Logger, LoggerFactory}

object DataFrameUtils {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  type FilterFunction = Row => Boolean

  type PartitionCriteria = Seq[(String, String)]

  def mapPartitionsToDirectories(partitionCriteria: PartitionCriteria): Seq[String] =
    partitionCriteria.map { case (columnName, columnValue) => s"$columnName=$columnValue" }

  def buildPartitionsCriteriaMatcherFunc(
      multiplePartitionsCriteria: Seq[PartitionCriteria],
      schema: StructType
  ): FilterFunction = {
    val targetPartitions = multiplePartitionsCriteria.flatten.map(_._1).toSet
    val fieldNameToMatchFunctionMapping =
      schema.fields
        .filter { case StructField(name, _, _, _) => targetPartitions.contains(name) }
        .map {
          case StructField(name, _: ByteType, _, _) =>
            name -> ((r: Row, value: String) => r.getAs[Byte](name) == value.toByte)
          case StructField(name, _: ShortType, _, _) =>
            name -> ((r: Row, value: String) => r.getAs[Short](name) == value.toShort)
          case StructField(name, _: IntegerType, _, _) =>
            name -> ((r: Row, value: String) => r.getAs[Int](name) == value.toInt)
          case StructField(name, _: LongType, _, _) =>
            name -> ((r: Row, value: String) => r.getAs[Long](name) == value.toLong)
          case StructField(name, _: FloatType, _, _) =>
            name -> ((r: Row, value: String) => r.getAs[Float](name) == value.toFloat)
          case StructField(name, _: DoubleType, _, _) =>
            name -> ((r: Row, value: String) => r.getAs[Double](name) == value.toDouble)
          case StructField(name, _: BooleanType, _, _) =>
            name -> ((r: Row, value: String) => r.getAs[Boolean](name) == value.toBoolean)
          case StructField(name, _: StringType, _, _) =>
            name -> ((r: Row, value: String) => r.getAs[String](name) == value)
          case StructField(_, dataType, _, _) =>
            throw new Exception("Unsupported partition data type: " + dataType.getClass)
        }
        .toMap

    def convertPartitionCriteriaToFilterFunctions(
        partitionCriteria: PartitionCriteria
    ): Seq[FilterFunction] =
      partitionCriteria.map {
        case (name, value) => (row: Row) => fieldNameToMatchFunctionMapping(name)(row, value)
      }

    def joinSinglePartitionFilterFunctionsWithAnd(
        partitionFilterFunctions: Seq[FilterFunction]
    ): FilterFunction =
      partitionFilterFunctions
        .reduceOption((predicate1, predicate2) => (row: Row) => predicate1(row) && predicate2(row))
        .getOrElse((_: Row) => false)

    multiplePartitionsCriteria
      .map(convertPartitionCriteriaToFilterFunctions)
      .map(joinSinglePartitionFilterFunctionsWithAnd)
      .reduceOption((predicate1, predicate2) => (row: Row) => predicate1(row) || predicate2(row))
      .getOrElse((_: Row) => false)
  }

  implicit class DataFrameHelper(df: DataFrame) {

    def collectPartitions(targetPartitions: Seq[String]): Seq[PartitionCriteria] = {
      logger.info(
        s"Collecting unique partitions for partitions columns (${targetPartitions.mkString(", ")})"
      )
      val partitions = df.selectExpr(targetPartitions: _*).distinct().collect()

      partitions.map { row =>
        targetPartitions.map { columnName =>
          Option(row.getAs[Any](columnName)) match {
            case Some(columnValue) => columnName -> columnValue.toString
            case None =>
              throw new RuntimeException(s"Partition column '$columnName' contains null value")
          }
        }
      }
    }

    def addMissingColumns(targetSchema: StructType): DataFrame = {
      val dataFieldsSet = df.schema.fieldNames.toSet
      val selectColumns = targetSchema.fields.map { field =>
        if (dataFieldsSet.contains(field.name)) functions.col(field.name)
        else functions.lit(null).cast(field.dataType).as(field.name)
      }
      df.select(selectColumns: _*)
    }

    def isEmpty: Boolean = df.head(1).isEmpty

    def nonEmpty: Boolean = df.head(1).nonEmpty
  }
}
