package com.adidas.analytics.feature.templates

import com.adidas.analytics.algo.loads.FullLoad
import com.adidas.analytics.util.{DFSWrapper, LoadMode}
import com.adidas.utils.TestUtils._
import com.adidas.utils.{BaseAlgorithmTest, FileReader, Table}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers._

class AlgorithmTemplateTest extends AnyFeatureSpec with BaseAlgorithmTest {

  private val sourceEnvironmentLocation: String = "test_landing"
  private val targetDatabase: String = "test_lake"
  private val tableName: String = "test_table"

  private val paramsFileName: String = "algorithm_template_params.json"

  private val paramsFileHdfsPath: Path = new Path(hdfsRootTestPath, paramsFileName)

  private val sourceDirPath: Path =
    new Path(hdfsRootTestPath, s"$sourceEnvironmentLocation/test/$tableName/data")

  private val targetDirPath: Path =
    new Path(hdfsRootTestPath, s"$targetDatabase/test/$tableName/data")

  Feature("Algorithm template successfully loads files to lake") {
    Scenario("when table is not partitioned, load is successful") {

      /** Implement here the steps required for the given test case.
        */
      copyResourceFileToHdfs(s"$paramsFileName", paramsFileHdfsPath)

      val targetSchema =
        DataType.fromJson(getResourceAsText("target_schema.json")).asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))

      val targetTable = createNonPartitionedTargetTable(targetSchema)
      setupInitialState(targetTable, "lake_data_pre.psv", dataReader)
      prepareDefaultSourceData()

      // checking pre-conditions
      spark.read.csv(sourceDirPath.toString).count() shouldBe 25
      targetTable.read().count() shouldBe 19

      FullLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      // validating result
      val expectedDataLocation = resolveResource("lake_data_post.psv", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = targetTable.read()
      actualDf.hasDiff(expectedDf) shouldBe false
    }
  }

  private def createNonPartitionedTargetTable(targetSchema: StructType): Table = {
    val targetTableLocation = fs.makeQualified(new Path(hdfsRootTestPath, targetDirPath)).toString
    Table
      .newBuilder(tableName, targetDatabase, targetTableLocation, targetSchema)
      .buildParquetTable(DFSWrapper(fs.getConf), spark, external = true)
  }

  private def setupInitialState(
      targetTable: Table,
      localDataFile: String,
      dataReader: FileReader
  ): Unit = {
    val initialDataLocation = resolveResource(localDataFile, withProtocol = true)
    targetTable
      .write(Seq(initialDataLocation), dataReader, LoadMode.OverwritePartitionsWithAddedColumns)
  }

  private def prepareDefaultSourceData(): Unit =
    Seq("new_data.psv").foreach { file =>
      logger.info(s"copyResourceFileToHdfs $file to ${sourceDirPath.toString}")
      copyResourceFileToHdfs(s"$file", sourceDirPath)
    }

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.sql(s"DROP DATABASE IF EXISTS $targetDatabase CASCADE")
    spark.sql(s"CREATE DATABASE $targetDatabase")
    logger.info(s"Creating ${sourceDirPath.toString}")
    fs.mkdirs(sourceDirPath)
    logger.info(s"Creating ${targetDirPath.toString}")
    fs.mkdirs(targetDirPath)
  }
}
