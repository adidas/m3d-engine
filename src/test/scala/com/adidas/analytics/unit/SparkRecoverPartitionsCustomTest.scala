package com.adidas.analytics.unit

import com.adidas.analytics.util.SparkRecoverPartitionsCustom
import com.adidas.utils.SparkSessionWrapper
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers, PrivateMethodTester}

import scala.collection.JavaConverters._

class SparkRecoverPartitionsCustomTest extends FunSuite
  with SparkSessionWrapper
  with PrivateMethodTester
  with Matchers
  with BeforeAndAfterAll{

  test("test conversion of String Value to HiveQL Partition Parameter") {
    val customSparkRecoverPartitions = SparkRecoverPartitionsCustom(tableName="", targetPartitions = Seq())
    val createParameterValue = PrivateMethod[String]('createParameterValue)
    val result = customSparkRecoverPartitions invokePrivate createParameterValue("theValue")

    result should be("'theValue'")
  }

  test("test conversion of Short Value to HiveQL Partition Parameter") {
    val customSparkRecoverPartitions = SparkRecoverPartitionsCustom(tableName="", targetPartitions = Seq())
    val createParameterValue = PrivateMethod[String]('createParameterValue)
    val result = customSparkRecoverPartitions invokePrivate createParameterValue(java.lang.Short.valueOf("2"))

    result should be("2")
  }

  test("test conversion of Integer Value to HiveQL Partition Parameter") {
    val customSparkRecoverPartitions = SparkRecoverPartitionsCustom(tableName="", targetPartitions = Seq())
    val createParameterValue = PrivateMethod[String]('createParameterValue)
    val result = customSparkRecoverPartitions invokePrivate createParameterValue(java.lang.Integer.valueOf("4"))

    result should be("4")
  }

  test("test conversion of null Value to HiveQL Partition Parameter") {
    val customSparkRecoverPartitions = SparkRecoverPartitionsCustom(tableName="", targetPartitions = Seq())
    val createParameterValue = PrivateMethod[String]('createParameterValue)
    an [Exception] should be thrownBy {
      customSparkRecoverPartitions invokePrivate createParameterValue(null)
    }
  }

  test("test conversion of not supported Value to HiveQL Partition Parameter") {
    val customSparkRecoverPartitions = SparkRecoverPartitionsCustom(tableName="", targetPartitions = Seq())
    val createParameterValue = PrivateMethod[String]('createParameterValue)
    an [Exception] should be thrownBy {
      customSparkRecoverPartitions invokePrivate createParameterValue(false)
    }
  }

  test("test HiveQL statements Generation") {
    val customSparkRecoverPartitions = SparkRecoverPartitionsCustom(
      tableName="test",
      targetPartitions = Seq("country","district")
    )

    val rowsInput = Seq(
      Row(1, "portugal", "porto"),
      Row(2, "germany", "herzogenaurach"),
      Row(3, "portugal", "coimbra")
    )

    val inputSchema = StructType(
      List(
        StructField("number", IntegerType, nullable = true),
        StructField("country", StringType, nullable = true),
        StructField("district", StringType, nullable = true)
      )
    )

    val expectedStatements: Seq[String] = Seq(
      "ALTER TABLE test ADD IF NOT EXISTS PARTITION(country='portugal',district='porto')",
      "ALTER TABLE test ADD IF NOT EXISTS PARTITION(country='germany',district='herzogenaurach')",
      "ALTER TABLE test ADD IF NOT EXISTS PARTITION(country='portugal',district='coimbra')"
    )

    val testDataset: Dataset[Row] = spark.createDataset(rowsInput)(RowEncoder(inputSchema))

    val createParameterValue = PrivateMethod[Dataset[String]]('generateAddPartitionStatements)

    val producedStatements: Seq[String] = (customSparkRecoverPartitions invokePrivate createParameterValue(testDataset))
      .collectAsList()
      .asScala

    expectedStatements.sorted.toSet should equal(producedStatements.sorted.toSet)
  }

  override def afterAll(): Unit = {
    spark.stop()
  }

}
