package com.adidas.analytics.feature

import com.adidas.utils.TestUtils._
import com.adidas.analytics.algo.NestedFlattener
import com.adidas.utils.{BaseAlgorithmTest, FileReader}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers._

class NestedFlattenerTest extends AnyFeatureSpec with BaseAlgorithmTest {

  private val database = "test_lake"
  private val paramsFileName = "params.json"

  private val paramsFileHdfsPath: Path = new Path(hdfsRootTestPath, paramsFileName)

  private val rootSourceDirPath: Path = new Path(hdfsRootTestPath, s"$database/nest")

  private val sourceDirPath: Path = new Path(hdfsRootTestPath, s"$database/nest/nest_test/data")
  private val sourceDataLocalDir = "nest_test"

  private val expectedTargetTableName = "expected_nest_flattened"
  private val expectedDataFileName = "expected_target_data.psv"

  private val targetTableName = "nest_flattened"

  private val targetDirPath: Path = new Path(hdfsRootTestPath, s"$database/$targetTableName/data")

  private val targetSchema =
    DataType.fromJson(getResourceAsText(s"target_schema.json")).asInstanceOf[StructType]

  Feature("Semi-structured data is fully flattened ... and problematic characters are replaced") {

    Scenario(
      "Test Case 1: target Schema is correct and non-partitioned table was successfully flattened and exploded"
    ) {
      val testCaseId = "scenario1"

      copyResourceFileToHdfs(s"$testCaseId/$paramsFileName", paramsFileHdfsPath)

      val sourceDf = spark.read.parquet(sourceDirPath.toString)

      // source table has the expected number of records
      sourceDf.count() shouldBe 3

      val targetTable = createParquetTable(database, targetTableName, schema = targetSchema)
      val nestedFlattener = NestedFlattener(spark, dfs, paramsFileHdfsPath.toString)
      nestedFlattener.run()

      // target table has correct schema
      targetTable.schema.equals(targetSchema) shouldBe true

      val targetDf = targetTable.read()

      val expectedTargetTable = createAndLoadParquetTable(
        database = database,
        tableName = expectedTargetTableName,
        schema = targetSchema,
        filePath = expectedDataFileName,
        reader = FileReader.newDSVFileReader(Some(targetSchema))
      )
      val expectedTargetDf = expectedTargetTable.read()

      // target table has exactly the same data as the expected data
      targetDf.hasDiff(expectedTargetDf) shouldBe false
    }

    Scenario(
      "Test Case 2: target Schema is correct and partitioned table was successfully flattened and exploded"
    ) {
      val testCaseId = "scenario2"

      copyResourceFileToHdfs(s"$testCaseId/$paramsFileName", paramsFileHdfsPath)

      val sourceDf = spark.read.parquet(sourceDirPath.toString)

      // source table has the expected number of records
      sourceDf.count() shouldBe 3

      val targetTable = createParquetTable(
        database,
        targetTableName,
        partitionColumns = Some(Seq("device_brand")),
        schema = targetSchema
      )
      val nestedFlattener = NestedFlattener(spark, dfs, paramsFileHdfsPath.toString)
      nestedFlattener.run()

      // target table has correct schema
      // columns have been renamed correctly
      targetTable.schema.equals(targetSchema) shouldBe true

      val targetDf = targetTable.read()

      val expectedTargetTable = createAndLoadParquetTable(
        database = database,
        tableName = expectedTargetTableName,
        partitionColumns = Some(Seq("device_brand")),
        schema = targetSchema,
        filePath = expectedDataFileName,
        reader = FileReader.newDSVFileReader(Some(targetSchema))
      )
      val expectedTargetDf = expectedTargetTable.read()

      // target table has exactly the same data as the expected data
      targetDf.hasDiff(expectedTargetDf) shouldBe false
    }

  }

  /* Creates the FS folders, sends the parameters and data to FS, and creates the database */
  override def beforeEach(): Unit = {
    super.beforeEach()

    fs.mkdirs(rootSourceDirPath)
    fs.mkdirs(targetDirPath)

    val sourceDataLocalDirPath = resolveResource(s"$sourceDataLocalDir")
    copyResourceFileToHdfs(sourceDataLocalDirPath, rootSourceDirPath)

    val targetDataFilePathLocal = resolveResource(s"$expectedDataFileName")
    copyResourceFileToHdfs(targetDataFilePathLocal, targetDirPath)

    spark.sql(s"DROP DATABASE IF EXISTS $database CASCADE")
    spark.sql(s"CREATE DATABASE $database")
  }
}
