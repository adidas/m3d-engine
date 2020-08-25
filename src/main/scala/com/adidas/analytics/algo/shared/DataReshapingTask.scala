package com.adidas.analytics.algo.shared

import com.adidas.analytics.algo.NestedFlattener
import com.adidas.analytics.algo.Transpose
import com.adidas.analytics.config.shared.DataReshapingTaskConfig
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

trait DataReshapingTask extends DataReshapingTaskConfig with DateComponentDerivation {

  /** Trait used to implement additional tasks on data during the transform stage. It picks up
    * properties via an ACON file. In the future, to include more tasks from other algorithms, you
    * need to assign a new match to the result variable, in order to look for other tasks in the
    * ACON file. intended or to call other algo functions
    *
    * @param dataFrame
    *   vector dataframe to process
    * @param spark
    *   spark session
    * @param targetSchema
    *   for schema verification
    */

  def additionalTasksDataFrame(
      spark: SparkSession,
      dataFrame: Vector[DataFrame],
      targetSchema: StructType,
      partitionSourceColumn: String,
      partitionSourceColumnFormat: String,
      targetPartitions: Seq[String]
  ): Vector[DataFrame] =
    try dataFrame.map { df =>
      Seq(
        NestedFlat(spark)(_),
        TransposeDf(spark, targetSchema)(_),
        withDatePartitions(partitionSourceColumn, partitionSourceColumnFormat, targetPartitions)(_),
        checkSchema(targetSchema)(_)
      ).foldLeft(df)((df, task) => df.transform(task))
    } catch {
      case e: Throwable => throw new RuntimeException("Unable to execute data reshaping task.", e)
    }

  def checkSchema(targetSchema: StructType)(df: DataFrame): DataFrame =
    if (enforceSchema) {
      var castedTargetCols = targetSchema.map(c => col(c.name).cast(c.dataType))
      targetSchema.fields.foreach { f =>
        if (!df.schema.fieldNames.contains(f.name))
          castedTargetCols = castedTargetCols.filter(_ != col(f.name).cast(f.dataType))
      }
      df.select(castedTargetCols: _*)
    } else df

  def withDatePartitions(
      partitionSourceColumn: String,
      partitionSourceColumnFormat: String,
      targetPartitions: Seq[String]
  )(df: DataFrame): DataFrame =
    if (targetPartitions.nonEmpty)
      df.transform(
        withDateComponents(partitionSourceColumn, partitionSourceColumnFormat, targetPartitions)
      )
    else df

  def TransposeDf(spark: SparkSession, targetSchema: StructType)(df: DataFrame): DataFrame =
    transposeTaskProperties match {
      case Some(_) =>
        Transpose.transposeTask(
          spark,
          df,
          getProperties[String]("transpose_task_properties", "pivot_column")
            .getOrElse(throw new RuntimeException("Pivot column value is missing")),
          getProperties[String]("transpose_task_properties", "aggregation_column")
            .getOrElse(throw new RuntimeException("Aggregation column value is missing")),
          getProperties[Seq[String]]("transpose_task_properties", "group_by_column")
            .getOrElse(throw new RuntimeException(" Group by value is missing")),
          targetSchema
        )
      case _ => df
    }

  def NestedFlat(spark: SparkSession)(df: DataFrame): DataFrame =
    flattenTaskProperties match {
      case Some(_) =>
        NestedFlattener.flatDataFrame(
          spark,
          NestedFlattener.replaceCharsInColumns(
            spark,
            df,
            getProperties[String]("nested_task_properties", "chars_to_replace")
              .getOrElse(throw new RuntimeException(s"replacement_char value is missing")),
            getProperties[String]("nested_task_properties", "replacement_char")
              .getOrElse(throw new RuntimeException(s"replacement_char value is missing"))
          ),
          getProperties[Seq[String]]("nested_task_properties", "fields_to_flatten")
            .getOrElse(throw new RuntimeException(s"fields_to_flatten value is missing")),
          getProperties[Map[String, String]]("nested_task_properties", "column_mapping")
            .getOrElse(throw new RuntimeException(s"column_mapping value is missing")),
          getProperties[Map[String, Seq[String]]]("nested_task_properties", "side_flatten")
        )
      case _ => df
    }

}
