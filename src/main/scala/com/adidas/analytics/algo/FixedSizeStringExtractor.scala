package com.adidas.analytics.algo

import com.adidas.analytics.algo.FixedSizeStringExtractor._
import com.adidas.analytics.algo.core.Algorithm
import com.adidas.analytics.config.FixedSizeStringExtractorConfiguration
import com.adidas.analytics.util.{DFSWrapper, DataFrameUtils}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType


final class FixedSizeStringExtractor protected(val spark: SparkSession, val dfs: DFSWrapper, val configLocation: String)
  extends Algorithm with FixedSizeStringExtractorConfiguration {

  override protected def transform(dataFrames: Vector[DataFrame]): Vector[DataFrame] = {
    val filteredDf = Option(partitionsCriteria).filter(_.nonEmpty).foldLeft(dataFrames(0)) {
      case (df, criteria) =>
        val isRequiredPartition = DataFrameUtils.buildPartitionsCriteriaMatcherFunc(Seq(criteria), df.schema)
        df.filter(isRequiredPartition)
    }

    val resultDf = filteredDf.transform(df => extractFields(df, targetSchema))
    Vector(resultDf)
  }

  def extractFields(df: DataFrame, targetSchema: StructType): DataFrame = {
    val nonPartitionFields = targetSchema.fields.filter(field => !targetPartitionsSet.contains(field.name))
    if (substringPositions.length != nonPartitionFields.length) {
      throw new RuntimeException("Field positions do not correspond to the target schema")
    }

    val sourceCol = col(sourceField)
    val extractedDf = nonPartitionFields.zip(substringPositions).foldLeft(df) {
      case (tempDf, (field, (startPos, endPos))) =>
        tempDf.withColumn(field.name, withExtractedString(sourceCol, startPos, endPos).cast(field.dataType))
    }

    extractedDf.selectExpr(targetSchema.fieldNames: _*)
  }
}


object FixedSizeStringExtractor {

  def apply(spark: SparkSession, dfs: DFSWrapper, configLocation: String): FixedSizeStringExtractor = {
    new FixedSizeStringExtractor(spark, dfs, configLocation)
  }

  private def withExtractedString(column: Column, startPos: Int, endPos: Int): Column = {
    udf[Option[String], String]((in: String) => Option(in.substring(startPos - 1, endPos).trim).filter(_.nonEmpty)).apply(column)
  }
}


