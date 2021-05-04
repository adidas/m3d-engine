package com.adidas.analytics.feature.loads

import com.adidas.analytics.algo.loads.DeltaLakeLoad
import com.adidas.analytics.util.{DFSWrapper, LoadMode}
import com.adidas.utils.TestUtils._
import com.adidas.utils.{BaseAlgorithmTest, FileReader, Table}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest.GivenWhenThen
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers._

class DeltaLakeLoadTest extends AnyFeatureSpec with BaseAlgorithmTest with GivenWhenThen {

  private val sourceSystem: String = "sales"
  private val testTableName: String = "orders"
  private val layerLanding: String = "test_landing"
  private val layerLake: String = "test_lake"

  private val paramsFile = "params.json"
  private val paramsFileHdfsPath: Path = new Path(hdfsRootTestPath, paramsFile)

  private val testInitDataFile = "init_data.psv"

  private val testLandingDeltaTableDirPath =
    new Path(hdfsRootTestPath, s"$layerLanding/$sourceSystem/$testTableName/delta_table")

  private val testNewDataFile = "new_data.psv"

  private val testLandingNewDataDirPath =
    new Path(hdfsRootTestPath, s"$layerLanding/$sourceSystem/$testTableName/data")

  private val testLakePath = new Path(hdfsRootTestPath, s"$layerLake")
  private val testLakePartitions = Seq("year", "month", "day")
  private val lakeSchemaName = "lake_schema.json"

  private val testControlDataFile = "control_data.psv"
  private val testControlTableName = "control"

  Feature(
    "Handle merges after init loads with duplicate rows with different record modes and dates"
  ) {

    Scenario("New data is available with new columns") {
      val resourceDir = "added_columns_and_duplicates_in_init"

      var targetTable = createTable(
        tableName = s"${sourceSystem}_$testTableName",
        targetPartitions = testLakePartitions,
        testResourceDir = resourceDir,
        schemaName = "lake_schema_initial.json"
      )

      Given("there was an init load of the delta table")
      And(
        "init load contains more than 1 row per business key with different record modes and null dates"
      )
      uploadNewDataToLanding(resourceDir, testInitDataFile)
      DeltaLakeLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      And("target table was then recreated with new schema that includes discount column")
      spark.sql(s"DROP TABLE IF EXISTS $layerLake.${sourceSystem}_$testTableName")
      targetTable = createTable(
        tableName = s"${sourceSystem}_$testTableName",
        targetPartitions = testLakePartitions,
        testResourceDir = resourceDir,
        schemaName = "lake_schema_final.json"
      )
      spark.catalog.recoverPartitions(s"$layerLake.${sourceSystem}_$testTableName")

      When("new delta data includes new discount column")
      uploadNewDataToLanding(resourceDir, testNewDataFile)
      DeltaLakeLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      Then("data in target table is correct")
      val controlTable = createTable(
        s"${sourceSystem}_${testTableName}_$testControlTableName",
        testLakePartitions,
        testResourceDir = resourceDir,
        Some(testControlDataFile),
        "lake_schema_final.json"
      )

      targetTable.read().hasDiff(controlTable.read()) shouldBe false
      spark.read.load(targetTable.location).schema shouldEqual targetTable.schema
    }

  }

  Feature("Regular Delta Lake Loads with (updates, inserts and deletes)") {

    Scenario("New data is available and target table is not partitioned") {
      val resourceDir = "nonpartitioned"

      copyResourceFileToHdfs(s"$resourceDir/$paramsFile", paramsFileHdfsPath)

      val targetTable = createTable(
        tableName = s"${sourceSystem}_$testTableName",
        targetPartitions = Seq(),
        testResourceDir = resourceDir,
        schemaName = lakeSchemaName
      )

      Given("there was an init load of the delta table")
      uploadNewDataToLanding(resourceDir, testInitDataFile)
      DeltaLakeLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      When("merge is executed in non partitioned delta table")
      uploadNewDataToLanding(resourceDir, testNewDataFile)
      DeltaLakeLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      Then("data in non partitioned target table is correct")
      val controlTable = createTable(
        s"${sourceSystem}_${testTableName}_$testControlTableName",
        Seq(),
        testResourceDir = resourceDir,
        Some(testControlDataFile),
        lakeSchemaName
      )

      targetTable.read().hasDiff(controlTable.read()) shouldBe false
      spark.read.load(targetTable.location).schema shouldEqual targetTable.schema
    }

    Scenario("New data is available with removed columns") {
      val resourceDir = "removed_columns"

      val targetTable = createTable(
        tableName = s"${sourceSystem}_$testTableName",
        targetPartitions = testLakePartitions,
        testResourceDir = resourceDir,
        schemaName = lakeSchemaName
      )

      Given("there was an init load of the delta table")
      uploadNewDataToLanding(resourceDir, testInitDataFile)
      DeltaLakeLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      When("new delta data does not include one column")
      uploadNewDataToLanding(resourceDir, testNewDataFile)
      DeltaLakeLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      Then("data in target table is correct")
      val controlTable = createTable(
        s"${sourceSystem}_${testTableName}_$testControlTableName",
        testLakePartitions,
        testResourceDir = resourceDir,
        Some(testControlDataFile),
        lakeSchemaName
      )

      targetTable.read().hasDiff(controlTable.read()) shouldBe false
      spark.read.load(targetTable.location).schema shouldEqual targetTable.schema
    }

  }

  Feature("Unstable target partitions whose values change over time for several rows") {

    Scenario(s"Unstable target partitions, and parameters are not properly configured") {
      Given("affected_partitions_merge is false")
      val resourceDir = "unstable_partitions_wrong_params"

      copyResourceFileToHdfs(s"$resourceDir/$paramsFile", paramsFileHdfsPath)

      val targetTable = createTable(
        tableName = s"${sourceSystem}_$testTableName",
        targetPartitions = testLakePartitions,
        testResourceDir = resourceDir,
        schemaName = lakeSchemaName
      )

      Given("there was an init load of the delta table")
      uploadNewDataToLanding(resourceDir, testInitDataFile)
      DeltaLakeLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      When("values for the target partition column have changed for several rows")
      uploadNewDataToLanding(resourceDir, testNewDataFile)
      DeltaLakeLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      val controlTable = createTable(
        s"${sourceSystem}_${testTableName}_$testControlTableName",
        testLakePartitions,
        testResourceDir = resourceDir,
        Some(testControlDataFile),
        lakeSchemaName
      )

      val targetDf = targetTable.read()
      val controlDf = controlTable.read()

      Then("there is two extra rows which should not be in the target table")
      targetDf.count() shouldBe controlDf.count() + 2

      And("target table does not match control table in terms of content")
      targetDf.hasDiff(controlDf) shouldBe true
      spark.read.load(targetTable.location).schema shouldEqual targetTable.schema
    }

    Scenario(s"Unstable target partitions, but parameters are properly configured") {
      val resourceDir = "unstable_partitions_right_params"

      Given("affected_partitions_merge is true")
      copyResourceFileToHdfs(s"$resourceDir/$paramsFile", paramsFileHdfsPath)

      val targetTable = createTable(
        tableName = s"${sourceSystem}_$testTableName",
        targetPartitions = testLakePartitions,
        testResourceDir = resourceDir,
        schemaName = lakeSchemaName
      )

      Given("there was an init load of the delta table")
      uploadNewDataToLanding(resourceDir, testInitDataFile)
      DeltaLakeLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      When("values for the target partition column have changed for several rows")
      uploadNewDataToLanding(resourceDir, testNewDataFile)
      DeltaLakeLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      Then("target table is as expected")
      val controlTable = createTable(
        s"${sourceSystem}_${testTableName}_$testControlTableName",
        testLakePartitions,
        testResourceDir = resourceDir,
        Some(testControlDataFile),
        lakeSchemaName
      )

      targetTable.read().hasDiff(controlTable.read()) shouldBe false
      spark.read.load(targetTable.location).schema shouldEqual targetTable.schema
    }

  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.sql(s"DROP DATABASE IF EXISTS $layerLake CASCADE")
    spark.sql(s"CREATE DATABASE $layerLake")

    fs.mkdirs(testLandingNewDataDirPath)
    fs.mkdirs(testLandingDeltaTableDirPath)
    fs.mkdirs(testLakePath)

    copyResourceFileToHdfs(paramsFile, paramsFileHdfsPath)
  }

  private def createTable(
      tableName: String,
      targetPartitions: Seq[String],
      testResourceDir: String,
      dsvFileName: Option[String] = None,
      schemaName: String
  ): Table = {
    val tableLocation = fs.makeQualified(new Path(testLakePath, tableName))
    val schema =
      DataType.fromJson(getResourceAsText(s"$testResourceDir/$schemaName")).asInstanceOf[StructType]

    val table = Table
      .newBuilder(tableName, layerLake, tableLocation.toString, schema)
      .withPartitions(targetPartitions)
      .buildParquetTable(DFSWrapper(fs.getConf), spark, external = true)

    if (dsvFileName.nonEmpty) {
      val dataLocation =
        resolveResource(s"$testResourceDir/${dsvFileName.get}", withProtocol = true)
      table.write(
        Seq(dataLocation),
        FileReader.newDSVFileReader(header = true),
        LoadMode.OverwritePartitionsWithAddedColumns,
        fillNulls = true
      )
    }

    table
  }

  private def uploadNewDataToLanding(testResourceDir: String, fileName: String): Unit = {
    fs.listStatus(testLandingNewDataDirPath).foreach(f => fs.delete(f.getPath, false))
    copyResourceFileToHdfs(s"$testResourceDir/$fileName", testLandingNewDataDirPath)
  }
}
