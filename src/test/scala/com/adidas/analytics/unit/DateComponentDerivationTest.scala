package com.adidas.analytics.unit

import com.adidas.utils.TestUtils._
import com.adidas.analytics.algo.shared.DateComponentDerivation
import com.adidas.utils.SparkSessionWrapper
import org.apache.spark.sql.DataFrame
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.funsuite.AnyFunSuite

class DateComponentDerivationTest
    extends AnyFunSuite
    with SparkSessionWrapper
    with Matchers
    with BeforeAndAfterAll {

  class DateComponentDerivationSubClass extends DateComponentDerivation {

    def validateWithDateComponents(
        sourceDateColumnName: String,
        sourceDateFormat: String,
        targetDateComponentColumnNames: Seq[String]
    )(inputDf: DataFrame): DataFrame =
      super
        .withDateComponents(sourceDateColumnName, sourceDateFormat, targetDateComponentColumnNames)(
          inputDf
        )

  }

  import spark.implicits._

  override def afterAll(): Unit = spark.stop()

  test("Partition by year/week with formatter yyyyww") {

    val sampleDf = Seq(("201301"), ("201531"), ("202001")).toDF("zatpweek")

    val dateComponentDerivationTester: DataFrame => DataFrame =
      new DateComponentDerivationSubClass().validateWithDateComponents(
        sourceDateColumnName = "zatpweek",
        sourceDateFormat = "yyyyww",
        targetDateComponentColumnNames = Seq("year", "week")
      )

    val transformedDf = sampleDf.transform(dateComponentDerivationTester)

    val expectedDf = Seq(("201301", 2013, 1), ("201531", 2015, 31), ("202001", 2020, 1))
      .toDF("zatpweek", "year", "week")

    transformedDf.hasDiff(expectedDf) shouldBe false
  }

  test("Partition by year/month/day with formatter yyyyMMdd") {

    val sampleDf = Seq(("20130112"), ("20150815"), ("20200325"), ("20180110")).toDF("partcol")

    val dateComponentDerivationTester: DataFrame => DataFrame =
      new DateComponentDerivationSubClass().validateWithDateComponents(
        sourceDateColumnName = "partcol",
        sourceDateFormat = "yyyyMMdd",
        targetDateComponentColumnNames = Seq("year", "month", "day")
      )

    val transformedDf = sampleDf.transform(dateComponentDerivationTester)

    val expectedDf = Seq(
      ("20130112", 2013, 1, 12),
      ("20150815", 2015, 8, 15),
      ("20200325", 2020, 3, 25),
      ("20180110", 2018, 1, 10)
    ).toDF("partcol", "year", "month", "day")

    transformedDf.hasDiff(expectedDf) shouldBe false
  }

  test("Partition by year/month with formatter yyyyMMdd") {

    val sampleDf = Seq(("20130112"), ("20150815"), ("20200325")).toDF("partcol")

    val dateComponentDerivationTester: DataFrame => DataFrame =
      new DateComponentDerivationSubClass().validateWithDateComponents(
        sourceDateColumnName = "partcol",
        sourceDateFormat = "yyyyMMdd",
        targetDateComponentColumnNames = Seq("year", "month")
      )

    val transformedDf = sampleDf.transform(dateComponentDerivationTester)

    val expectedDf = Seq(("20130112", 2013, 1), ("20150815", 2015, 8), ("20200325", 2020, 3))
      .toDF("partcol", "year", "month")

    transformedDf.hasDiff(expectedDf) shouldBe false
  }

  test("Partition by year/month with formatter yyyyMMdd - with wrong data") {

    val sampleDf = Seq(("20130112"), ("201508151"), ("20200325")).toDF("partcol")

    val dateComponentDerivationTester: DataFrame => DataFrame =
      new DateComponentDerivationSubClass().validateWithDateComponents(
        sourceDateColumnName = "partcol",
        sourceDateFormat = "yyyyMMdd",
        targetDateComponentColumnNames = Seq("year", "month")
      )

    val transformedDf = sampleDf.transform(dateComponentDerivationTester)

    val expectedDf = Seq(("20130112", 2013, 1), ("201508151", 9999, 99), ("20200325", 2020, 3))
      .toDF("partcol", "year", "month")

    transformedDf.hasDiff(expectedDf) shouldBe false
  }

  test("Partition by year/month with formatter yyyyMM as IntegerType") {

    val sampleDf = Seq((201301), (2015233), (202003)).toDF("partcol")

    val dateComponentDerivationTester: DataFrame => DataFrame =
      new DateComponentDerivationSubClass().validateWithDateComponents(
        sourceDateColumnName = "partcol",
        sourceDateFormat = "yyyyMM",
        targetDateComponentColumnNames = Seq("year", "month")
      )

    val transformedDf = sampleDf.transform(dateComponentDerivationTester)
    val expectedDf = Seq((201301, 2013, 1), (2015233, 9999, 99), (202003, 2020, 3))
      .toDF("partcol", "year", "month")

    transformedDf.hasDiff(expectedDf) shouldBe false
  }

  test("Partition by year/week/day with formatter yyyywwe as IntegerType") {

    val sampleDf = Seq((2013014), (2015233), (2020037)).toDF("partcol")

    val dateComponentDerivationTester: DataFrame => DataFrame =
      new DateComponentDerivationSubClass().validateWithDateComponents(
        sourceDateColumnName = "partcol",
        sourceDateFormat = "yyyywwe",
        targetDateComponentColumnNames = Seq("year", "week", "day")
      )

    val transformedDf = sampleDf.transform(dateComponentDerivationTester)

    val expectedDf = Seq((2013014, 2013, 1, 4), (2015233, 2015, 23, 3), (2020037, 2020, 3, 7))
      .toDF("partcol", "year", "week", "day")

    transformedDf.hasDiff(expectedDf) shouldBe false
  }

  test("Partition by year/month/day with formatter MM/dd/yyyy") {

    val sampleDf = Seq(("10/31/2020"), ("05/07/2020"), ("12/15/2020")).toDF("partcol")

    val dateComponentDerivationTester: DataFrame => DataFrame =
      new DateComponentDerivationSubClass().validateWithDateComponents(
        sourceDateColumnName = "partcol",
        sourceDateFormat = "MM/dd/yyyy",
        targetDateComponentColumnNames = Seq("year", "month", "day")
      )

    val transformedDf = sampleDf.transform(dateComponentDerivationTester)

    val expectedDf =
      Seq(("10/31/2020", 2020, 10, 31), ("05/07/2020", 2020, 5, 7), ("12/15/2020", 2020, 12, 15))
        .toDF("partcol", "year", "month", "day")

    transformedDf.hasDiff(expectedDf) shouldBe false
  }

}
