package com.adidas.analytics.algo

import com.adidas.analytics.algo.core.Algorithm
import com.adidas.analytics.config.AlgorithmTemplateConfiguration
import com.adidas.analytics.util.{DFSWrapper}
import org.apache.spark.sql._

final class AlgorithmTemplate protected(val spark: SparkSession, val dfs: DFSWrapper, val configLocation: String)
  extends Algorithm with AlgorithmTemplateConfiguration {

  override protected def transform(dataFrames: Vector[DataFrame]): Vector[DataFrame] = {
    /**
      * In this method perform all the operations required to obtain the desired dataframe.
      * For example, adding new columns, calculating values for columns, exploding, etc.
      *
      * @param dataFrames this would be a 3-D array, where each cell of the Vector has a 2-D spark dataframe
      */

    throw new NotImplementedError("This class is not meant to be used. Please, considering implementing your own class based on this template")
  }
}


object AlgorithmTemplate {

  /**
    * Additionally, one can define a companion object, with different attributes and methods.
    * These methods could be helpers for the transform method.
    * In this case, an instantiation of AlgorithmTemplate occurs in the companion object.
    *
    * @param spark instance of SparkSession class.
    * @param dfs instance of DFSWrapper class for FS operations helper.
    * @param configLocation path of configuration file for the algorithm.
    * @return
    */
  def apply(spark: SparkSession, dfs: DFSWrapper, configLocation: String): AlgorithmTemplate = {
    new AlgorithmTemplate(spark, dfs, configLocation)
  }
}
