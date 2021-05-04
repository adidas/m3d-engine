package com.adidas.utils

import java.io.File
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.Logger
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

trait SparkSupport extends SparkSessionWrapper {

  def logger: Logger

  def testAppId: String

  def localTestDir: String

  override def startSpark(hadoopConf: Option[Configuration] = None): SparkSession = {
    // This line makes it possible to use a remote debugger
    System.setSecurityManager(null)

    val appDir = new File(localTestDir, testAppId)
    val sparkTestDir = new File(appDir, "spark").getAbsoluteFile
    sparkTestDir.mkdirs()

    val sparkConf = hadoopConf.foldLeft {
      new SparkConf(false)
        .set("spark.ui.enabled", "false")
        .set("spark.sql.warehouse.dir", new File(sparkTestDir, "warehouse").getAbsolutePath)
        .set("spark.sql.shuffle.partitions", "2")
    } { (sparkConf, hadoopConf) =>
      hadoopConf.asScala.foldLeft(sparkConf)((sc, entry) =>
        sc.set(s"spark.hadoop.${entry.getKey}", entry.getValue)
      )
    }

    System.setProperty("derby.system.home", new File(sparkTestDir, "metastore").getAbsolutePath)

    logger.info(s"Starting Spark Session with warehouse dir at ${sparkTestDir.getAbsolutePath} ...")
    SparkSession
      .builder()
      .config(sparkConf)
      .appName(s"test-${getClass.getName}")
      .master("local[2]")
      .enableHiveSupport()
      .getOrCreate()
  }

  def addHadoopConfiguration(conf: Configuration): Unit =
    conf.asScala.foreach { property =>
      spark.sparkContext.hadoopConfiguration.set(property.getKey, property.getValue)
    }

  def addHadoopProperty(key: String, value: String): Unit =
    spark.sparkContext.hadoopConfiguration.set(key, value)

}
