package com.adidas.analytics.algo

import com.adidas.analytics.algo.core.JobRunner
import org.apache.spark.sql.SparkSession

final class SQLRunner(spark: SparkSession, configLocation: String) extends JobRunner {

  override def run(): Unit = {
    // parse parameter file
    val algoParams = spark.read.option("multiline", "true").json(configLocation).cache()
    val steps = algoParams.select("steps").collect()(0)(0).toString

    // execute steps provided in parameter file
    for (i <- 0 until steps.toInt) {
      val sql = algoParams.select("" + i).collect()(0)(0).toString
      val df = spark.sql(sql)
      df.show(1000, truncate = false)
    }
  }
}

object SQLRunner {

  def apply(spark: SparkSession, configLocation: String): SQLRunner =
    new SQLRunner(spark, configLocation)
}
