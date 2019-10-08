package com.adidas.analytics.algo

import com.adidas.analytics.algo.core.Algorithm
import com.adidas.analytics.config.PartitionMaterializationConfiguration
import com.adidas.analytics.config.PartitionMaterializationConfiguration._
import com.adidas.analytics.config.shared.ConfigurationContext
import com.adidas.analytics.util._
import org.apache.spark.sql._

/**
  * Performs loading of partitions from existing view/table to a specified location overwriting existing data
  */
trait PartitionMaterialization extends Algorithm with PartitionMaterializationConfiguration {

  override protected def transform(dataFrames: Vector[DataFrame]): Vector[DataFrame] = {
    val result = Option(partitionsCriteria).filter(_.nonEmpty).foldLeft(dataFrames(0)) {
      case (df, partitionsCriteria) =>
        val isRequiredPartition = DataFrameUtils.buildPartitionsCriteriaMatcherFunc(partitionsCriteria, df.schema)
        df.filter(isRequiredPartition)
    }

    Vector(result)
  }
}


object PartitionMaterialization {

  def newFullMaterialization(spark: SparkSession, dfs: DFSWrapper, configLocation: String): PartitionMaterialization = {
    new FullMaterialization(spark, dfs, LoadMode.OverwriteTable, configLocation)
  }

  def newRangeMaterialization(spark: SparkSession, dfs: DFSWrapper, configLocation: String): PartitionMaterialization = {
    new RangeMaterialization(spark, dfs, LoadMode.OverwritePartitionsWithAddedColumns, configLocation)
  }

  def newQueryMaterialization(spark: SparkSession, dfs: DFSWrapper, configLocation: String): PartitionMaterialization = {
    new QueryMaterialization(spark, dfs, LoadMode.OverwritePartitionsWithAddedColumns, configLocation)
  }

  private class FullMaterialization(val spark: SparkSession, val dfs: DFSWrapper, val loadMode: LoadMode, val configLocation: String)
    extends PartitionMaterialization with FullMaterializationConfiguration with ConfigurationContext {
  }

  private class RangeMaterialization(val spark: SparkSession, val dfs: DFSWrapper, val loadMode: LoadMode, val configLocation: String)
    extends PartitionMaterialization with RangeMaterializationConfiguration with ConfigurationContext {
  }

  private class QueryMaterialization(val spark: SparkSession, val dfs: DFSWrapper, val loadMode: LoadMode, val configLocation: String)
    extends PartitionMaterialization with QueryMaterializationConfiguration with ConfigurationContext {
  }
}