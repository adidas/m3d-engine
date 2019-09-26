package com.adidas.analytics.algo

import com.adidas.analytics.TestUtils._
import com.adidas.analytics.util.{DFSWrapper, LoadMode}
import com.adidas.analytics.{FileReader, Table}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest.FeatureSpec
import org.scalatest.Matchers._


class PartitionMaterializationTest extends FeatureSpec with BaseAlgorithmTest {

  private val paramsFileName: String = "params.json"
  private val paramsFileHdfsPath: Path = new Path(hdfsRootTestPath, paramsFileName)

  private val sourceDatabase: String = "test_mart_mod"
  private val targetDatabase: String = "test_mart_cal"

  private var sourceTable: Table = _
  private var targetTable: Table = _
  private var fileReader: FileReader = _


  feature("Partitions should be loaded for ranges of dates if correct partitioning schema is specified") {
    scenario("When partitioning schema is year/month/day") {
      val testDir = "range_materialization/year_month_day_single_day"
      prepareTables(testDir, Seq("year", "month", "day"))
      copyResourceFileToHdfs(s"$testDir/$paramsFileName", paramsFileHdfsPath)

      // check pre-conditions
      sourceTable.read().count() shouldBe 19
      targetTable.read().count() shouldBe 0
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=2/day=15")) shouldBe false

      // execute load
      PartitionMaterialization.newRangeMaterialization(spark, dfs, paramsFileHdfsPath.toUri.toString).run()

      // compare data
      val actualDf = targetTable.read()
      val expectedDf = fileReader.read(spark, getExpectedDataFile(testDir))
      expectedDf.hasDiff(actualDf) shouldBe false

      // check that new partition was created
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=2/day=15")) shouldBe true
    }

    scenario("When partitioning schema is year/month") {
      val testDir = "range_materialization/year_month"
      prepareTables(testDir, Seq("year", "month"))
      copyResourceFileToHdfs(s"$testDir/$paramsFileName", paramsFileHdfsPath)

      // check pre-conditions
      sourceTable.read().count() shouldBe 19
      targetTable.read().count() shouldBe 0
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=2")) shouldBe false

      // execute load
      PartitionMaterialization.newRangeMaterialization(spark, dfs, paramsFileHdfsPath.toUri.toString).run()

      // compare data
      val actualDf = targetTable.read()
      val expectedDf = fileReader.read(spark, getExpectedDataFile(testDir))
      expectedDf.hasDiff(actualDf) shouldBe false

      // check that new partition was created
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=2")) shouldBe true
    }

    scenario("When partitioning schema is year/week") {
      val testDir = "range_materialization/year_week"
      prepareTables(testDir, Seq("year", "week"))
      copyResourceFileToHdfs(s"$testDir/$paramsFileName", paramsFileHdfsPath)

      // adding data to target table
      targetTable.write(Seq(getInitialDataFile(testDir)), fileReader, LoadMode.OverwritePartitions)

      // check pre-conditions
      sourceTable.read().count() shouldBe 19
      targetTable.read().count() shouldBe 2
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/week=1")) shouldBe false
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/week=2")) shouldBe false
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/week=3")) shouldBe false
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/week=4")) shouldBe false

      // execute load
      PartitionMaterialization.newRangeMaterialization(spark, dfs, paramsFileHdfsPath.toUri.toString).run()

      // compare data
      val actualDf = targetTable.read()
      val expectedDf = fileReader.read(spark, getExpectedDataFile(testDir))
      expectedDf.hasDiff(actualDf) shouldBe false

      // check that new partition was created
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/week=1")) shouldBe true
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/week=2")) shouldBe true
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/week=3")) shouldBe true
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/week=4")) shouldBe true
    }

    scenario("When partitioning schema is year/week/day") {
      val testDir = "range_materialization/year_week_day"
      prepareTables(testDir, Seq("year", "week", "day"))
      copyResourceFileToHdfs(s"$testDir/$paramsFileName", paramsFileHdfsPath)

      // execute load
      assertThrows[RuntimeException] {
        PartitionMaterialization.newRangeMaterialization(spark, dfs, paramsFileHdfsPath.toUri.toString).run()
      }
    }

    scenario("When the same partition exists in the target table") {
      val testDir = "range_materialization/year_month_day_single_day"
      prepareTables(testDir, Seq("year", "month", "day"))
      copyResourceFileToHdfs(s"$testDir/$paramsFileName", paramsFileHdfsPath)

      // add data to target table
      targetTable.write(Seq(Seq(9999, 1111, "", 20170215, "CUSTOMER99", "ARTICLE", 99, 2017, 2, 15)), LoadMode.OverwritePartitions)

      // checking pre-conditions
      sourceTable.read().count() shouldBe 19
      targetTable.read().count() shouldBe 1
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=2/day=15")) shouldBe true

      // execute load
      PartitionMaterialization.newRangeMaterialization(spark, dfs, paramsFileHdfsPath.toUri.toString).run()

      // compare data
      val actualDf = targetTable.read()
      val expectedDf = fileReader.read(spark, getExpectedDataFile(testDir))
      expectedDf.hasDiff(actualDf) shouldBe false

      // check that the partition exists
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=2/day=15")) shouldBe true
    }

    scenario("When other partitions exist in the target table") {
      val testDir = "range_materialization/year_month_day_single_day"
      prepareTables(testDir, Seq("year", "month", "day"))
      copyResourceFileToHdfs(s"$testDir/$paramsFileName", paramsFileHdfsPath)

      // adding data to target table
      targetTable.write(Seq(getInitialDataFile(testDir)), fileReader, LoadMode.OverwritePartitions)

      // check pre-conditions
      sourceTable.read().count() shouldBe 19
      targetTable.read().count() shouldBe 2
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=3/day=15")) shouldBe true
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=6/day=15")) shouldBe true

      // execute load
      PartitionMaterialization.newRangeMaterialization(spark, dfs, paramsFileHdfsPath.toUri.toString).run()

      // compare data
      val actualDf = targetTable.read()
      val newPartitionDf = fileReader.read(spark, getExpectedDataFile(testDir))
      val existingPartitionsDf = fileReader.read(spark, getInitialDataFile(testDir))
      val expectedDf = newPartitionDf.union(existingPartitionsDf)
      actualDf.hasDiff(expectedDf) shouldBe false

      // check that new partition was created
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=2/day=15")) shouldBe true
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=3/day=15")) shouldBe true
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=6/day=15")) shouldBe true
    }

    scenario("When a range of multiple days is specified in the job configuration") {
      val testDir = "range_materialization/year_month_day_multiple_days"
      prepareTables(testDir, Seq("year", "month", "day"))
      copyResourceFileToHdfs(s"$testDir/$paramsFileName", paramsFileHdfsPath)

      // check pre-conditions
      sourceTable.read().count() shouldBe 19
      targetTable.read().count() shouldBe 0
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=2/day=15")) shouldBe false
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=2/day=16")) shouldBe false
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=2/day=17")) shouldBe false

      // execute load
      PartitionMaterialization.newRangeMaterialization(spark, dfs, paramsFileHdfsPath.toUri.toString).run()

      // compare data
      val actualDf = targetTable.read()
      val expectedDf = fileReader.read(spark, getExpectedDataFile(testDir))
      expectedDf.hasDiff(actualDf) shouldBe false

      // check that new partition was created
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=2/day=15")) shouldBe true
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=2/day=16")) shouldBe true
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=2/day=17")) shouldBe true
    }
  }

  feature("Partitions should be loaded if they correspond to the specified conditions") {
    scenario("When the same partition does not exist on the filesystem") {
      val testDir = "condition_materialization"
      prepareTables(testDir, Seq("year", "month", "day"))
      copyResourceFileToHdfs(s"$testDir/$paramsFileName", paramsFileHdfsPath)

      // check pre-conditions
      sourceTable.read().count() shouldBe 19
      targetTable.read().count() shouldBe 0
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=2/day=15")) shouldBe false

      // execute load
      PartitionMaterialization.newQueryMaterialization(spark, dfs, paramsFileHdfsPath.toUri.toString).run()

      // compare data
      val actualDf = targetTable.read()
      val expectedDf = fileReader.read(spark, getExpectedDataFile(testDir))
      expectedDf.hasDiff(actualDf) shouldBe false

      // check that new partition was created
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=2/day=15")) shouldBe true
    }

    scenario("When the same partition exists on the filesystem") {
      val testDir = "condition_materialization"
      prepareTables(testDir, Seq("year", "month", "day"))
      copyResourceFileToHdfs(s"$testDir/$paramsFileName", paramsFileHdfsPath)

      // add data to target table
      targetTable.write(Seq(Seq(9999, 1111, "", 20170215, "CUSTOMER99", "ARTICLE", 99, 2017, 2, 15)), LoadMode.OverwritePartitions)

      // checking pre-conditions
      sourceTable.read().count() shouldBe 19
      targetTable.read().count() shouldBe 1
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=2/day=15")) shouldBe true

      // execute load
      PartitionMaterialization.newQueryMaterialization(spark, dfs, paramsFileHdfsPath.toUri.toString).run()

      // compare data
      val actualDf = targetTable.read()
      val expectedDf = fileReader.read(spark, getExpectedDataFile(testDir))
      expectedDf.hasDiff(actualDf) shouldBe false

      // check that the partition exists
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=2/day=15")) shouldBe true
    }

    scenario("When other partitions exist on the filesystem") {
      val testDir = "condition_materialization"
      prepareTables(testDir, Seq("year", "month", "day"))
      copyResourceFileToHdfs(s"$testDir/$paramsFileName", paramsFileHdfsPath)

      // adding data to target table
      targetTable.write(Seq(getInitialDataFile(testDir)), fileReader, LoadMode.OverwritePartitions)

      // check pre-conditions
      sourceTable.read().count() shouldBe 19
      targetTable.read().count() shouldBe 2
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=3/day=15")) shouldBe true
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=6/day=15")) shouldBe true

      // execute load
      PartitionMaterialization.newQueryMaterialization(spark, dfs, paramsFileHdfsPath.toUri.toString).run()

      // compare data
      val actualDf = targetTable.read()
      val newPartitionDf = fileReader.read(spark, getExpectedDataFile(testDir))
      val intermediatePartitionsDf = fileReader.read(spark, getInitialDataFile(testDir))
      val expectedDf = newPartitionDf.union(intermediatePartitionsDf)
      actualDf.hasDiff(expectedDf) shouldBe false

      // check that new partition was created
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=2/day=15")) shouldBe true
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=3/day=15")) shouldBe true
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=6/day=15")) shouldBe true
    }
  }

  feature("The number of output partitions should be configurable") {
    scenario("When number of output partitions is 3") {
      val testDir = "condition_materialization"
      prepareTables(testDir, Seq("year", "month", "day"))
      copyResourceFileToHdfs(s"output_files_3/$paramsFileName", paramsFileHdfsPath)

      // adding data to target table
      targetTable.write(Seq(getInitialDataFile(testDir)), fileReader, LoadMode.OverwritePartitions)

      // check pre-conditions
      sourceTable.read().count() shouldBe 19
      targetTable.read().count() shouldBe 2
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=3/day=15")) shouldBe true
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=6/day=15")) shouldBe true

      // execute load
      PartitionMaterialization.newQueryMaterialization(spark, dfs, paramsFileHdfsPath.toUri.toString).run()

      // compare data
      val actualDf = targetTable.read()
      val newPartitionDf = fileReader.read(spark, getExpectedDataFile(testDir))
      val intermediatePartitionsDf = fileReader.read(spark, getInitialDataFile(testDir))
      val expectedDf = newPartitionDf.union(intermediatePartitionsDf)
      actualDf.hasDiff(expectedDf) shouldBe false

      // check that new partition was created
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=2/day=15")) shouldBe true
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=3/day=15")) shouldBe true
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=6/day=15")) shouldBe true
      fs.listStatus(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=2/day=15")).length shouldBe 3
    }

    scenario("When number of output partitions is 5") {
      val testDir = "condition_materialization"
      prepareTables(testDir, Seq("year", "month", "day"))
      copyResourceFileToHdfs(s"output_files_5/$paramsFileName", paramsFileHdfsPath)

      // adding data to target table
      targetTable.write(Seq(getInitialDataFile(testDir)), fileReader, LoadMode.OverwritePartitions)

      // check pre-conditions
      sourceTable.read().count() shouldBe 19
      targetTable.read().count() shouldBe 2
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=3/day=15")) shouldBe true
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=6/day=15")) shouldBe true

      // execute load
      PartitionMaterialization.newQueryMaterialization(spark, dfs, paramsFileHdfsPath.toUri.toString).run()

      // compare data
      val actualDf = targetTable.read()
      val newPartitionDf = fileReader.read(spark, getExpectedDataFile(testDir))
      val intermediatePartitionsDf = fileReader.read(spark, getInitialDataFile(testDir))
      val expectedDf = newPartitionDf.union(intermediatePartitionsDf)
      actualDf.hasDiff(expectedDf) shouldBe false

      // check that new partition was created
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=2/day=15")) shouldBe true
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=3/day=15")) shouldBe true
      fs.exists(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=6/day=15")) shouldBe true
      fs.listStatus(new Path(hdfsRootTestPath, "target_table_data/year=2017/month=2/day=15")).length shouldBe 5
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.sql(s"DROP DATABASE IF EXISTS $targetDatabase CASCADE")
    spark.sql(s"DROP DATABASE IF EXISTS $sourceDatabase CASCADE")
    spark.sql(s"CREATE DATABASE $sourceDatabase")
    spark.sql(s"CREATE DATABASE $targetDatabase")
  }

  private def getSourceDataFile(testDir: String): String = {
    resolveResource(s"$testDir/source_data.psv", withProtocol = true)
  }

  private def getInitialDataFile(testDir: String): String = {
    resolveResource(s"$testDir/initial_data.psv", withProtocol = true)
  }

  private def getExpectedDataFile(testDir: String): String = {
    resolveResource(s"$testDir/expected_data.psv", withProtocol = true)
  }

  private def prepareTables(testDir: String, partitions: Seq[String]): Unit = {
    val schema = DataType.fromJson(getResourceAsText(s"$testDir/schema.json")).asInstanceOf[StructType]

    // create file reader with the current schema
    fileReader = FileReader.newDSVFileReader(optionalSchema = Some(schema))

    // create source table and add data to it
    sourceTable = createTable("source_table", sourceDatabase, partitions, "source_table_data", schema)
    sourceTable.write(Seq(getSourceDataFile(testDir)), fileReader, LoadMode.OverwritePartitions)

    // create target table
    targetTable = createTable("target_table", targetDatabase, partitions, "target_table_data", schema)
  }

  private def createTable(table: String, database: String, partitions: Seq[String], tableDirName: String, schema: StructType): Table = {
    Table.newBuilder(table, database, fs.makeQualified(new Path(hdfsRootTestPath, tableDirName)).toString, schema)
      .withPartitions(partitions)
      .buildParquetTable(DFSWrapper(fs.getConf), spark, external = true)
  }
}
