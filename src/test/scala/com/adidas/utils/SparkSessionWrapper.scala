package com.adidas.utils

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._

trait SparkSessionWrapper {

  lazy val spark: SparkSession = startSpark(sparkHadoopConf)

  def sparkHadoopConf: Option[Configuration] = Option.empty

  def startSpark(hadoopConf: Option[Configuration]): SparkSession = {

    val sparkConf = hadoopConf.foldLeft {
      new SparkConf(false)
    } { (sparkConf, hadoopConf) =>
      hadoopConf.foldLeft(sparkConf)((sc, entry) => sc.set(entry.getKey, entry.getValue))
    }

    SparkSession.builder()
      .config(sparkConf)
      .appName("spark tests")
      .master("local[*]")
      .getOrCreate()
  }

}
