package com.adidas.analytics.integration

import com.adidas.analytics.algo.loads.AppendLoad
import com.adidas.utils.TestUtils._
import com.adidas.utils.FileReader
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Dataset, Encoders}
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers._

class RecoverPartitionsCustomIntegrationTest extends AnyFeatureSpec with BaseIntegrationTest {

  Feature("Partitions can be updated programmatically using custom logic") {

    Scenario("Using Append Load Algorithm with multiple source files") {
      val testResourceDir = "multiple_source_files"
      val headerPath20180101 = new Path(headerDirPath, "year=2018/month=1/day=1/header.json")
      val targetPath20180101 = new Path(targetDirPath, "year=2018/month=1/day=1")

      val targetSchema =
        DataType
          .fromJson(getResourceAsText(s"$testResourceDir/target_schema.json"))
          .asInstanceOf[StructType]
      val expectedPartitionsSchema =
        DataType
          .fromJson(getResourceAsText(s"$testResourceDir/expected_partitions_schema.json"))
          .asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))
      val expectedPartitionsDataReader = FileReader.newDSVFileReader(Some(expectedPartitionsSchema))

      val targetTable = createTargetTable(Seq("year", "month", "day"), targetSchema)
      setupInitialState(targetTable, s"$testResourceDir/lake_data_pre.psv", dataReader)
      prepareSourceData(
        testResourceDir,
        Seq("data_20180101-part-00000.psv", "data_20180101-part-00001.psv")
      )
      uploadParameters(testResourceDir)

      // checking pre-conditions
      spark.read.csv(sourceDirPath.toString).count() shouldBe 7
      targetTable.read().count() shouldBe 19

      fs.exists(targetPath20180101) shouldBe false
      fs.exists(headerPath20180101) shouldBe false

      // executing load
      AppendLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      // validating result
      val expectedDataLocation =
        resolveResource(s"$testResourceDir/lake_data_post.psv", withProtocol = true)
      val expectedPartitionsLocation =
        resolveResource(s"$testResourceDir/expected_partitions.txt", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = targetTable.read()

      val producedPartitionsNumber: Dataset[String] =
        spark.sql(s"SHOW PARTITIONS $targetDatabase.$tableName").as(Encoders.STRING)

      // MetaData Specific Tests
      val expectedPartitions: Dataset[String] =
        expectedPartitionsDataReader.read(spark, expectedPartitionsLocation).as(Encoders.STRING)

      expectedPartitions.collect().sorted.toSet should
        equal(producedPartitionsNumber.collect().sorted.toSet)

      actualDf.hasDiff(expectedDf) shouldBe false

      spark
        .sql(s"DESCRIBE extended $targetDatabase.$tableName PARTITION(year=2018,month=1,day=1)")
        .filter("col_name == 'Partition Statistics'")
        .head()
        .getAs[String]("data_type")
        .contains("6 rows") shouldBe true

      fs.exists(targetPath20180101) shouldBe true
      fs.exists(headerPath20180101) shouldBe true
    }
  }

}
