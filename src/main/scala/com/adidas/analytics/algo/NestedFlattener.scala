package com.adidas.analytics.algo

import com.adidas.analytics.algo.core.Algorithm
import com.adidas.analytics.config.NestedFlattenerConfiguration
import com.adidas.analytics.util.DFSWrapper
import org.apache.spark.sql.functions.{col, explode_outer}
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * An algorithm for flattening semi-structured JSON data in a configurable way, i.e., giving the user the ability to choose
  * which struct fields should be flattened or array fields should be exploded by the algorithm.
  *
  * @param spark          spark session
  * @param dfs            distributed file system
  * @param configLocation location of the configuration file for the algorithm
  */
final class NestedFlattener protected(val spark: SparkSession, val dfs: DFSWrapper, val configLocation: String) extends Algorithm with NestedFlattenerConfiguration {

  override protected def transform(dataFrames: Vector[DataFrame]): Vector[DataFrame] = {
    val inputDf = dataFrames(0)
    val replacedDf = NestedFlattener.replaceCharsInColumns(spark, inputDf, charsToReplace, replacementChar)
    val flattenedDf = NestedFlattener.flatDataFrame(spark, replacedDf, fieldsToFlatten, columnMapping)
    Vector(flattenedDf)
  }

}

object NestedFlattener {

  def apply(spark: SparkSession, dfs: DFSWrapper, configLocation: String): NestedFlattener = {
    new NestedFlattener(spark, dfs, configLocation)
  }

  /**
    * Replaces problematic characters present in the semi-structured file format (e.g., JSON), such as "." which makes spark think that the field is a struct.
    * Note: needs to run before any flattening attempt, that's why the transform function of this algorithm executes this step first.
    * Moreover don't forget to consider the charsToReplace in the name mapping in the acon file, because these chars will be replaced by the replacementChar.
    *
    * @param spark           spark session
    * @param df              dataframe to process
    * @param charsToReplace  problematic column name characters to be replaced
    * @param replacementChar char that replaces the charsToReplace
    * @return a dataframe with the column names cleansed of problematic characters
    */
  def replaceCharsInColumns(spark: SparkSession, df: DataFrame, charsToReplace: String, replacementChar: String): DataFrame = {

    def changeSchemaNames(f: StructField): StructField = {
      val cleansedName = f.name.replaceAll(charsToReplace, replacementChar)
      f.dataType match {
        case st: StructType =>
          val children = st.fields.map(f => changeSchemaNames(f))
          StructField(cleansedName, StructType(children), f.nullable, f.metadata)
        case at: ArrayType =>
          val childrenDataType = changeSchemaNames(StructField("element", at.elementType)).dataType
          StructField(cleansedName, ArrayType(childrenDataType, at.containsNull), f.nullable, f.metadata)
        case _ =>
          StructField(cleansedName, f.dataType, f.nullable, f.metadata)
      }
    }

    val schema = StructType(df.schema.fields.map(f => changeSchemaNames(f)))
    spark.createDataFrame(df.rdd, schema)
  }

  /**
    * Flattens a given DataFrame according to the attributes (arrays or structs) to process.
    * Important Note: the chars separating parent and child fieldnames in the flattened attributes is two underscores,
    * so make sure you consider this in the name mapping config in the acon file.
    *
    * @param spark           spark session
    * @param df              dataframe to be processed
    * @param fieldsToFlatten fields to include for the flattening process. Note: you should specify not only top-level attributes but sub-levels as well
    *                        if you want them included.
    * @param columnMapping   columns to include in the final dataframe and with new (more friendly) names. Note: columns not in the columnMapping will be excluded
    * @return flattened DataFrame according to the configuration of the algorithm
    */
  def flatDataFrame(spark: SparkSession, df: DataFrame, fieldsToFlatten: Seq[String], columnMapping: Map[String, String]): DataFrame = {

    def dropFieldIfNotForFlattening(df: DataFrame, fieldName: String, callback: () => DataFrame): DataFrame = {
      if (fieldsToFlatten.contains(fieldName))
        callback()
      else
        df.drop(fieldName)
    }

    @scala.annotation.tailrec
    def flatDataFrameAux(df: DataFrame): DataFrame = {

      var auxDf = df
      var continueFlat = false

      auxDf.schema.fields.foreach(f => {
        f.dataType match {
          case _: ArrayType =>
            auxDf = dropFieldIfNotForFlattening(auxDf, f.name, () => {
              val columnsWithoutArray = auxDf.schema.fieldNames
                .filter(_ != f.name)
                .map(n => col(n))
              val columnsWithExplode = columnsWithoutArray ++ Array(explode_outer(col(f.name)).as(f.name))
              continueFlat = true
              auxDf.select(columnsWithExplode: _*)
            })

          case st: StructType =>
            auxDf = dropFieldIfNotForFlattening(auxDf, f.name, () => {
              // renames all struct fields to have full names and removes original struct root
              val fullPathNames = st.fieldNames.map(n => f.name + "." + n)
              val columnNamesWithoutStruct = auxDf.schema.fieldNames.filter(_ != f.name) ++ fullPathNames
              val renamedColumns = columnNamesWithoutStruct.map(n => col(n).as(n.replace(".", "__")))
              continueFlat = true
              auxDf.select(renamedColumns: _*)
            })

          case _ => // do nothing
        }
      })

      if (continueFlat)
        flatDataFrameAux(auxDf)
      else
        auxDf
    }

    // Rename fields according to columnMapping and drop columns that are not mapped
    var flattenedDf = flatDataFrameAux(df)
    flattenedDf.schema.foreach(f => {
      if (columnMapping.contains(f.name))
        flattenedDf = flattenedDf.withColumnRenamed(f.name, columnMapping(f.name))
      else
        flattenedDf = flattenedDf.drop(f.name)
    })

    flattenedDf
  }

}




