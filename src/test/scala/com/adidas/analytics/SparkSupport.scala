package com.adidas.analytics

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger

import scala.collection.JavaConversions._


trait SparkSupport {

  lazy val spark: SparkSession = startSpark(sparkHadoopConf)

  def logger: Logger
  def testAppId: String
  def localTestDir: String
  def sparkHadoopConf: Option[Configuration] = Option.empty

  def startSpark(hadoopConf: Option[Configuration]): SparkSession = {
    // This line makes it possible to use a remote debugger
    System.setSecurityManager(null)

    val appDir = new File(localTestDir, testAppId)
    val sparkTestDir = new File(appDir, "spark").getAbsoluteFile
    sparkTestDir.mkdirs()

    val sparkConf = hadoopConf.foldLeft {
      new SparkConf(false)
        .set("spark.ui.enabled", "false")
        .set("spark.sql.warehouse.dir", new File(sparkTestDir, "warehouse").getAbsolutePath)
    } { (sparkConf, hadoopConf) =>
      hadoopConf.foldLeft(sparkConf)((sc, entry) => sc.set(s"spark.hadoop.${entry.getKey}", entry.getValue))
    }

    System.setProperty("derby.system.home", new File(sparkTestDir, "metastore").getAbsolutePath)

    logger.info(s"Staring Spark Session with warehouse dir at ${sparkTestDir.getAbsolutePath} ...")
    SparkSession.builder()
      .config(sparkConf)
      .appName(s"test-${getClass.getName}")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
  }

  def addHadoopConfiguration(conf: Configuration): Unit = {
    conf.foreach { property =>
      spark.sparkContext.hadoopConfiguration.set(property.getKey, property.getValue)
    }
  }

  def addHadoopProperty(key: String, value: String): Unit = {
    spark.sparkContext.hadoopConfiguration.set(key, value)
  }
}
