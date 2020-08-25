package com.adidas.analytics

import com.adidas.analytics.algo._
import com.adidas.analytics.algo.core.JobRunner
import com.adidas.analytics.algo.loads.{AppendLoad, DeltaLakeLoad, DeltaLoad, FullLoad}
import com.adidas.analytics.util.DFSWrapper
import org.apache.spark.sql.SparkSession

/** Driver class to initiate execution
  */
object AlgorithmFactory {

  // number of required arguments
  private val ArgumentNumber = 2

  def main(args: Array[String]): Unit = {
    // check parameters
    if (args.length < ArgumentNumber) {
      Console.err.println("Usage: [appClassName] [appParamFile]")
      sys.exit(0)
    }

    // get parameters
    val appClassName = args(0)
    val appParamFile = args(1)

    // get Spark session
    val spark = createSparkSession(appClassName)

    try createAlgorithmInstance(spark, appClassName, appParamFile).run()
    finally spark.close()
  }

  /** Create an instance of SparkSession with all the necessary parameters
    */
  private def createSparkSession(appClassName: String): SparkSession =
    SparkSession
      .builder()
      .appName(appClassName)
      .config("hive.cbo.enable", "true")
      .config("hive.compute.query.using.stats", "true")
      .config("hive.exec.parallel", "true")
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.stats.fetch.column.stats", "true")
      .config("hive.stats.fetch.partition.stats", "true")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .config("spark.sql.parquet.writeLegacyFormat", "true")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .config("spark.sql.csv.parser.columnPruning.enabled", "false")
      .enableHiveSupport()
      .getOrCreate()

  /** Select algorithm to execute and create an instance of it
    */
  private def createAlgorithmInstance(
      spark: SparkSession,
      className: String,
      configLocation: String
  ): JobRunner = {
    val dfs = DFSWrapper(spark.sparkContext.hadoopConfiguration)

    className match {
      case "AppendLoad"               => AppendLoad(spark, dfs, configLocation)
      case "DeltaLoad"                => DeltaLoad(spark, dfs, configLocation)
      case "DeltaLakeLoad"            => DeltaLakeLoad(spark, dfs, configLocation)
      case "FixedSizeStringExtractor" => FixedSizeStringExtractor(spark, dfs, configLocation)
      case "FullLoad"                 => FullLoad(spark, dfs, configLocation)
      case "FullMaterialization" =>
        Materialization.newFullMaterialization(spark, dfs, configLocation)
      case "GzipDecompressorBytes" => GzipDecompressor(spark, dfs, configLocation)
      case "SQLRunner"             => SQLRunner(spark, configLocation)
      case "NestedFlattener"       => NestedFlattener(spark, dfs, configLocation)
      case "QueryMaterialization" =>
        Materialization.newQueryMaterialization(spark, dfs, configLocation)
      case "RangeMaterialization" =>
        Materialization.newRangeMaterialization(spark, dfs, configLocation)
      case "Transpose" => Transpose(spark, dfs, configLocation)
      case _           => throw new RuntimeException(s"Unable to find algorithm corresponding to $className")
    }
  }
}
