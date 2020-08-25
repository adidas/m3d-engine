package com.adidas.analytics.algo

import com.adidas.analytics.algo.core.Algorithm
import com.adidas.analytics.config.TransposeConfiguration
import com.adidas.analytics.util.DFSWrapper
import org.apache.spark.sql.functions.{col, first}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

final class Transpose protected (
    val spark: SparkSession,
    val dfs: DFSWrapper,
    val configLocation: String
) extends Algorithm
    with TransposeConfiguration {

  override protected def transform(dataFrames: Vector[DataFrame]): Vector[DataFrame] = {

    val inputDf = dataFrames(0)
    val transposeDf = Transpose
      .transposeTask(spark, inputDf, pivotColumn, aggregationColumn, groupByColumn, targetSchema)
    val result =
      if (enforceSchema) {
        var castedTargetCols = targetSchema.map(c => col(c.name).cast(c.dataType))
        targetSchema.fields.foreach { f =>
          if (!transposeDf.schema.fieldNames.contains(f.name))
            castedTargetCols = castedTargetCols.filter(_ != col(f.name).cast(f.dataType))
        }
        transposeDf.select(castedTargetCols: _*)
      } else transposeDf
    Vector(result)
  }
}

object Transpose {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def apply(spark: SparkSession, dfs: DFSWrapper, configLocation: String): Transpose =
    new Transpose(spark, dfs, configLocation)

  /** Transposes a given DataFrame using a group by and pivot operation. The columns used on that
    * operations are passed using the acon file. Important Note: the char separating parent and
    * child fieldnames in the flattened attributes is two underscores, so make sure you consider
    * this in the name mapping config in the acon file.
    *
    * @param spark
    *   spark session
    * @param df
    *   dataframe to be processed
    * @param pivotColumn
    *   column from the data frame input used to rotate the data frame
    * @param aggrColumn
    *   column used to aggregate the values under a row
    * @param groupColumn
    *   column or sequence of columns used to group by the data frame
    * @param targetSchema
    *   final schema to remove columns or the flatten data frame and not on the final
    * @return
    *   flattened and transposed DataFrame according to the configuration of the algorithm
    */

  def transposeTask(
      spark: SparkSession,
      df: DataFrame,
      pivotColumn: String,
      aggrColumn: String,
      groupColumn: Seq[String],
      targetSchema: StructType
  ): DataFrame = {

    logger.info("Transposing source data")
    val result = df
      .filter(col(pivotColumn).isNotNull)
      .groupBy(groupColumn map col: _*)
      .pivot(pivotColumn, targetSchema.map(c => c.name).diff(groupColumn))
      .agg(first(col(aggrColumn)))
    result
  }

}
