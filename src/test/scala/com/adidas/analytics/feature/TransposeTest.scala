package com.adidas.analytics.feature

import com.adidas.utils.TestUtils._
import com.adidas.analytics.algo.Transpose
import com.adidas.utils.{BaseAlgorithmTest, FileReader, Table}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers._

class TransposeTest extends AnyFeatureSpec with BaseAlgorithmTest {

  private val lake_database = "test_lake"
  private val paramsFileName = "params.json"

  private val paramsFileHdfsPath: Path = new Path(hdfsRootTestPath, paramsFileName)
  private val sourceDataLocalDir = "input_data.psv"
  private val expectedTargetTableName = "expected_pos_transpose"
  private val expectedDataFileName = "expected_target_data.psv"

  private val sourceSchema: StructType =
    DataType.fromJson(getResourceAsText("source_schema.json")).asInstanceOf[StructType]
  private val targetTableName = "pos_transpose"

  private val targetDirPath: Path =
    new Path(hdfsRootTestPath, s"$lake_database/$targetTableName/data")

  private val targetSchema =
    DataType.fromJson(getResourceAsText(s"target_schema.json")).asInstanceOf[StructType]
  private val sourceTableName = "pre_transpose"

  private var sourceTable: Table = _

  Feature("Transpose  Algorithm from table") {
    Scenario("Simple transformation non partitioned table") {
      val sourceDf = sourceTable.read()
      // source and mart table have the expected number of records
      sourceDf.count() shouldBe 6
      val targetTable = createParquetTable(lake_database, targetTableName, schema = targetSchema)

      val TransposeTransformation = Transpose(spark, dfs, paramsFileHdfsPath.toString)
      TransposeTransformation.run()

      // target table has correct schema
      targetTable.schema.equals(targetSchema) shouldBe true

      val targetDf = targetTable.read()
      val expectedTargetTable = createAndLoadParquetTable(
        database = lake_database,
        tableName = expectedTargetTableName,
        schema = targetSchema,
        filePath = expectedDataFileName,
        reader = FileReader.newDSVFileReader(Some(targetSchema))
      )

      val expectedTargetDf = expectedTargetTable.read()

      // target table has exactly the same data as the expected data
      targetDf.hasDiff(expectedTargetDf) shouldBe false
    }
  }

  /* Creates the FS folders, sends the parameters and data to FS, and creates the mart_database */
  override def beforeEach(): Unit = {
    super.beforeEach()
    fs.mkdirs(targetDirPath)
    spark.sql(s"DROP DATABASE IF EXISTS $lake_database CASCADE")
    spark.sql(s"CREATE DATABASE $lake_database")

    // copy job parameters to HDFS
    copyResourceFileToHdfs(paramsFileName, paramsFileHdfsPath)
    // create tables
    sourceTable = createAndLoadParquetTable(
      lake_database,
      sourceTableName,
      None,
      schema = sourceSchema,
      sourceDataLocalDir,
      FileReader.newDSVFileReader(Some(sourceSchema))
    )
  }

}
