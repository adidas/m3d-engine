package com.adidas.analytics.integration

import com.adidas.analytics.algo.loads.FullLoad
import com.adidas.utils.TestUtils._
import com.adidas.analytics.util.CatalogTableManager
import com.adidas.utils.{FileReader, Table}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Dataset, Encoders}
import org.scalatest.matchers.should.Matchers._
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.Assertion
import scala.util.{Failure, Success, Try}

class FailFastIntegrationTest extends AnyFeatureSpec with BaseIntegrationTest {

  override val sourceDirPath: Path =
    new Path(hdfsRootTestPath, s"$sourceDatabase/test/$tableName/data")

  override val targetDirPath: Path =
    new Path(hdfsRootTestPath, s"$targetDatabase/test/$tableName/data")

  protected val backupDirPath: Path =
    new Path(hdfsRootTestPath, s"$targetDatabase/test/$tableName/data_backup")

  Feature("FailFast Option should fail safely regarding data and metadata") {

    Scenario("Full Load Algorithm running in FailFast mode and failing safely!") {
      val resourceDir = "partitioned"
      copyResourceFileToHdfs(s"$resourceDir/$paramsFileName", paramsFileHdfsPath)

      val targetPath20180110 = new Path("year=2018/month=1/day=10")
      val targetSchema = DataType
        .fromJson(getResourceAsText(s"$resourceDir/target_schema.json"))
        .asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))
      val expectedPartitionsSchema =
        DataType
          .fromJson(getResourceAsText(s"$resourceDir/expected_partitions_schema.json"))
          .asInstanceOf[StructType]
      val expectedPartitionsDataReader = FileReader.newDSVFileReader(Some(expectedPartitionsSchema))

      val targetTable =
        createPartitionedTargetTable(Seq("year", "month", "day"), targetSchema, tableName)

      // Populate the table with data and Partitions
      integrationTestStep(
        sourceDataFile = "landing/new_data.psv",
        resourceDir = resourceDir,
        targetPath = targetPath20180110,
        shouldFail = false,
        dataReader = dataReader,
        metadataReader = expectedPartitionsDataReader,
        targetTable = targetTable
      )

      // Wrong Data Should not affect table data and partitioning
      integrationTestStep(
        sourceDataFile = "landing/new_data_wrong_format.psv",
        resourceDir = resourceDir,
        targetPath = targetPath20180110,
        shouldFail = true,
        dataReader = dataReader,
        metadataReader = expectedPartitionsDataReader,
        targetTable = targetTable
      )

    }

  }

  private def integrationTestStep(
      sourceDataFile: String,
      shouldFail: Boolean,
      resourceDir: String,
      targetPath: Path,
      dataReader: FileReader,
      metadataReader: FileReader,
      targetTable: Table
  ): Assertion = {
    prepareDefaultSourceData(sourceDataFile)

    // executing load
    val isPipelineFailing = Try(FullLoad(spark, dfs, paramsFileHdfsPath.toString).run()) match {
      case Failure(_) => true
      case Success(_) => false
    }

    isPipelineFailing should equal(shouldFail)

    // validating result
    val expectedDataLocation =
      resolveResource(s"$resourceDir/lake_data_post.psv", withProtocol = true)
    val expectedDf = dataReader.read(spark, expectedDataLocation)
    val actualDf = targetTable.read()

    actualDf.hasDiff(expectedDf) shouldBe false
    fs.exists(
      new Path(CatalogTableManager(targetTable.table, spark).getTableLocation, targetPath)
    ) shouldBe true

    // MetaData Specific Tests
    val producedPartitionsNumber: Dataset[String] =
      spark.sql(s"SHOW PARTITIONS $targetDatabase.$tableName").as(Encoders.STRING)

    val expectedPartitionsLocation =
      resolveResource(s"$resourceDir/expected_partitions.txt", withProtocol = true)
    val expectedPartitions: Dataset[String] =
      metadataReader.read(spark, expectedPartitionsLocation).as(Encoders.STRING)

    expectedPartitions
      .collect()
      .toSet
      .diff(producedPartitionsNumber.collect().toSet) should equal(Set())

  }
}
