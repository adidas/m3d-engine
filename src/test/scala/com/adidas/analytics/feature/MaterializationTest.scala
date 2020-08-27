package com.adidas.analytics.feature

import com.adidas.analytics.algo.Materialization
import com.adidas.analytics.util.{DFSWrapper, CatalogTableManager, LoadMode}
import com.adidas.utils.TestUtils._
import com.adidas.utils.{BaseAlgorithmTest, FileReader, Table}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers._

class MaterializationTest extends AnyFeatureSpec with BaseAlgorithmTest {

  private val paramsFileName: String = "params.json"

  private val paramsFileHdfsPath: Path = new Path(hdfsRootTestPath, paramsFileName)

  private val sourceDatabase: String = "test_mart_mod"
  private val targetDatabase: String = "test_mart_cal"

  private var sourceTable: Table = _
  private var targetTable: Table = _
  private var fileReader: FileReader = _

  Feature("View should be materialized in full") {
    Scenario("When there is no partitioning scheme and old materialization leftovers") {
      val testDir = "full_materialization"
      prepareTables(testDir, Seq.empty[String])
      copyResourceFileToHdfs(s"$testDir/no_partitions/$paramsFileName", paramsFileHdfsPath)

      // check pre-conditions
      sourceTable.read().count() shouldBe 19
      targetTable.read().count() shouldBe 0

      //manually create previous versions of the materialized view
      val tableDataPath = new Path(hdfsRootTestPath, s"target_table/data/")
      val tableLocations = createPreviousVersionsAndLeftovers(tableDataPath)

      var numFilesInTableDataDir = fs.listStatus(tableDataPath).count(_ => true)
      numFilesInTableDataDir shouldBe 16

      // execute materialization
      Materialization.newFullMaterialization(spark, dfs, paramsFileHdfsPath.toUri.toString).run()

      // compare data
      val actualDf = targetTable.read()
      val expectedDf = fileReader.read(spark, getExpectedDataFile(testDir))
      actualDf.hasDiff(expectedDf) shouldBe false

      /* table data folder contains only the new materialization plus the default previous versions */
      val finalTableLocation = CatalogTableManager(targetTable.table, spark).getTableLocation
      val fileStatus = fs.listStatus(tableDataPath)

      numFilesInTableDataDir = fileStatus.count(_ => true)
      numFilesInTableDataDir shouldBe 5

      fs.exists(tableLocations.head) shouldBe false
      fs.exists(tableLocations(1)) shouldBe false
      fs.exists(tableLocations(2)) shouldBe false
      fs.exists(tableLocations(3)) shouldBe false
      fs.exists(tableLocations(4)) shouldBe false
      fs.exists(tableLocations(5)) shouldBe false
      fs.exists(tableLocations(6)) shouldBe false
      fs.exists(tableLocations(7)) shouldBe false
      fs.exists(tableLocations(8)) shouldBe true
      fs.exists(tableLocations(9)) shouldBe true

      /* most recent subfolder is the table location and its parent folder is as expected */
      new Path(finalTableLocation).getParent.getName shouldBe tableDataPath.getName
      val mostRecentSubFolder =
        fileStatus.map(_.getPath.toString).toSeq.sorted(Ordering.String.reverse).head
      (mostRecentSubFolder == finalTableLocation) shouldBe true
    }

    Scenario("When view is partitioned and there was a previous materialization") {
      val testDir = "full_materialization"
      prepareTables(testDir, Seq("year", "month", "day"))
      copyResourceFileToHdfs(s"$testDir/$paramsFileName", paramsFileHdfsPath)

      // check pre-conditions
      sourceTable.read().count() shouldBe 19
      targetTable.read().count() shouldBe 0

      //manually create previous versions of the materialized view
      val tableDataPath = new Path(hdfsRootTestPath, s"target_table/data/")
      val tableLocations = createPreviousVersionsAndLeftovers(tableDataPath)

      var numFilesInTableDataDir = fs.listStatus(tableDataPath).count(_ => true)
      numFilesInTableDataDir shouldBe 16

      /* execute two materializations (wait two seconds between them to avoid folders with same
       * timestamp) */
      /* to check if there is no problem with a materialized view that is not initially pointing to
       * base_data_dir */
      Materialization.newFullMaterialization(spark, dfs, paramsFileHdfsPath.toUri.toString).run()
      Thread.sleep(2000)
      Materialization.newFullMaterialization(spark, dfs, paramsFileHdfsPath.toUri.toString).run()

      // compare data
      val actualDf = targetTable.read()
      val expectedDf = fileReader.read(spark, getExpectedDataFile(testDir))
      actualDf.hasDiff(expectedDf) shouldBe false

      /* table data folder contains the new materialization plus the 3 previous versions */
      val finalTableLocation = CatalogTableManager(targetTable.table, spark).getTableLocation
      val fileStatus = fs.listStatus(tableDataPath)

      numFilesInTableDataDir = fileStatus.count(_ => true)
      numFilesInTableDataDir shouldBe 6

      fs.exists(tableLocations.head) shouldBe false
      fs.exists(tableLocations(1)) shouldBe false
      fs.exists(tableLocations(2)) shouldBe false
      fs.exists(tableLocations(3)) shouldBe false
      fs.exists(tableLocations(4)) shouldBe false
      fs.exists(tableLocations(5)) shouldBe false
      fs.exists(tableLocations(6)) shouldBe false
      fs.exists(tableLocations(7)) shouldBe false
      fs.exists(tableLocations(8)) shouldBe true
      fs.exists(tableLocations(9)) shouldBe true

      // check that partitions were created
      fs.exists(new Path(finalTableLocation, "year=2016")) shouldBe true
      fs.exists(new Path(finalTableLocation, "year=2017")) shouldBe true
      fs.exists(new Path(finalTableLocation, "year=2018")) shouldBe true

      /* most recent subfolder is the table location and its parent folder is as expected */
      new Path(finalTableLocation).getParent.getName shouldBe tableDataPath.getName
      val mostRecentSubFolder =
        fileStatus.map(_.getPath.toString).toSeq.sorted(Ordering.String.reverse).head
      (mostRecentSubFolder == finalTableLocation) shouldBe true
    }
  }

  Feature(
    "Partitions should be loaded for ranges of dates if correct partitioning schema is specified"
  ) {
    Scenario("When partitioning schema is year/month/day") {
      val testDir = "range_materialization/year_month_day_single_day"
      prepareTables(testDir, Seq("year", "month", "day"))
      copyResourceFileToHdfs(s"$testDir/$paramsFileName", paramsFileHdfsPath)

      // check pre-conditions
      sourceTable.read().count() shouldBe 19
      targetTable.read().count() shouldBe 0
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=2/day=15")) shouldBe
        false

      // execute load
      Materialization.newRangeMaterialization(spark, dfs, paramsFileHdfsPath.toUri.toString).run()

      // compare data
      val actualDf = targetTable.read()
      val expectedDf = fileReader.read(spark, getExpectedDataFile(testDir))
      expectedDf.hasDiff(actualDf) shouldBe false

      // check that new partition was created
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=2/day=15")) shouldBe
        true
    }

    Scenario("When partitioning schema is year/month") {
      val testDir = "range_materialization/year_month"
      prepareTables(testDir, Seq("year", "month"))
      copyResourceFileToHdfs(s"$testDir/$paramsFileName", paramsFileHdfsPath)

      // check pre-conditions
      sourceTable.read().count() shouldBe 19
      targetTable.read().count() shouldBe 0
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=2")) shouldBe false

      // execute load
      Materialization.newRangeMaterialization(spark, dfs, paramsFileHdfsPath.toUri.toString).run()

      // compare data
      val actualDf = targetTable.read()
      val expectedDf = fileReader.read(spark, getExpectedDataFile(testDir))
      expectedDf.hasDiff(actualDf) shouldBe false

      // check that new partition was created
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=2")) shouldBe true
    }

    Scenario("When partitioning schema is year/week") {
      val testDir = "range_materialization/year_week"
      prepareTables(testDir, Seq("year", "week"))
      copyResourceFileToHdfs(s"$testDir/$paramsFileName", paramsFileHdfsPath)

      // adding data to target table
      targetTable.write(
        Seq(getInitialDataFile(testDir)),
        fileReader,
        LoadMode.OverwritePartitionsWithAddedColumns
      )

      // check pre-conditions
      sourceTable.read().count() shouldBe 19
      targetTable.read().count() shouldBe 2
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/week=1")) shouldBe false
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/week=2")) shouldBe false
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/week=3")) shouldBe false
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/week=4")) shouldBe false

      // execute load
      Materialization.newRangeMaterialization(spark, dfs, paramsFileHdfsPath.toUri.toString).run()

      // compare data
      val actualDf = targetTable.read()
      val expectedDf = fileReader.read(spark, getExpectedDataFile(testDir))
      expectedDf.hasDiff(actualDf) shouldBe false

      // check that new partition was created
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/week=1")) shouldBe true
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/week=2")) shouldBe true
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/week=3")) shouldBe true
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/week=4")) shouldBe true
    }

    Scenario("When partitioning schema is year/week/day") {
      val testDir = "range_materialization/year_week_day"
      prepareTables(testDir, Seq("year", "week", "day"))
      copyResourceFileToHdfs(s"$testDir/$paramsFileName", paramsFileHdfsPath)

      // execute load
      assertThrows[RuntimeException] {
        Materialization.newRangeMaterialization(spark, dfs, paramsFileHdfsPath.toUri.toString).run()
      }
    }

    Scenario("When the same partition exists in the target table") {
      val testDir = "range_materialization/year_month_day_single_day"
      prepareTables(testDir, Seq("year", "month", "day"))
      copyResourceFileToHdfs(s"$testDir/$paramsFileName", paramsFileHdfsPath)

      // add data to target table
      targetTable.write(
        Seq(Seq(9999, 1111, "", 20170215, "CUSTOMER99", "ARTICLE", 99, 2017, 2, 15)),
        LoadMode.OverwritePartitionsWithAddedColumns
      )

      // checking pre-conditions
      sourceTable.read().count() shouldBe 19
      targetTable.read().count() shouldBe 1
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=2/day=15")) shouldBe
        true

      // execute load
      Materialization.newRangeMaterialization(spark, dfs, paramsFileHdfsPath.toUri.toString).run()

      // compare data
      val actualDf = targetTable.read()
      val expectedDf = fileReader.read(spark, getExpectedDataFile(testDir))
      expectedDf.hasDiff(actualDf) shouldBe false

      // check that the partition exists
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=2/day=15")) shouldBe
        true
    }

    Scenario("When other partitions exist in the target table") {
      val testDir = "range_materialization/year_month_day_single_day"
      prepareTables(testDir, Seq("year", "month", "day"))
      copyResourceFileToHdfs(s"$testDir/$paramsFileName", paramsFileHdfsPath)

      // adding data to target table
      targetTable.write(
        Seq(getInitialDataFile(testDir)),
        fileReader,
        LoadMode.OverwritePartitionsWithAddedColumns
      )

      // check pre-conditions
      sourceTable.read().count() shouldBe 19
      targetTable.read().count() shouldBe 2
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=3/day=15")) shouldBe
        true
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=6/day=15")) shouldBe
        true

      // execute load
      Materialization.newRangeMaterialization(spark, dfs, paramsFileHdfsPath.toUri.toString).run()

      // compare data
      val actualDf = targetTable.read()
      val newPartitionDf = fileReader.read(spark, getExpectedDataFile(testDir))
      val existingPartitionsDf = fileReader.read(spark, getInitialDataFile(testDir))
      val expectedDf = newPartitionDf.union(existingPartitionsDf)
      actualDf.hasDiff(expectedDf) shouldBe false

      // check that new partition was created
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=2/day=15")) shouldBe
        true
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=3/day=15")) shouldBe
        true
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=6/day=15")) shouldBe
        true
    }

    Scenario("When a range of multiple days is specified in the job configuration") {
      val testDir = "range_materialization/year_month_day_multiple_days"
      prepareTables(testDir, Seq("year", "month", "day"))
      copyResourceFileToHdfs(s"$testDir/$paramsFileName", paramsFileHdfsPath)

      // check pre-conditions
      sourceTable.read().count() shouldBe 19
      targetTable.read().count() shouldBe 0
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=2/day=15")) shouldBe
        false
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=2/day=16")) shouldBe
        false
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=2/day=17")) shouldBe
        false

      // execute load
      Materialization.newRangeMaterialization(spark, dfs, paramsFileHdfsPath.toUri.toString).run()

      // compare data
      val actualDf = targetTable.read()
      val expectedDf = fileReader.read(spark, getExpectedDataFile(testDir))
      expectedDf.hasDiff(actualDf) shouldBe false

      // check that new partition was created
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=2/day=15")) shouldBe
        true
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=2/day=16")) shouldBe
        true
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=2/day=17")) shouldBe
        true
    }
  }

  Feature("Partitions should be loaded if they correspond to the specified conditions") {
    Scenario("When the same partition does not exist on the filesystem") {
      val testDir = "query_materialization"
      prepareTables(testDir, Seq("year", "month", "day"))
      copyResourceFileToHdfs(s"$testDir/$paramsFileName", paramsFileHdfsPath)

      // check pre-conditions
      sourceTable.read().count() shouldBe 19
      targetTable.read().count() shouldBe 0
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=2/day=15")) shouldBe
        false

      // execute load
      Materialization.newQueryMaterialization(spark, dfs, paramsFileHdfsPath.toUri.toString).run()

      // compare data
      val actualDf = targetTable.read()
      val expectedDf = fileReader.read(spark, getExpectedDataFile(testDir))
      expectedDf.hasDiff(actualDf) shouldBe false

      // check that new partition was created
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=2/day=15")) shouldBe
        true
    }

    Scenario("When the same partition exists on the filesystem") {
      val testDir = "query_materialization"
      prepareTables(testDir, Seq("year", "month", "day"))
      copyResourceFileToHdfs(s"$testDir/$paramsFileName", paramsFileHdfsPath)

      // add data to target table
      targetTable.write(
        Seq(Seq(9999, 1111, "", 20170215, "CUSTOMER99", "ARTICLE", 99, 2017, 2, 15)),
        LoadMode.OverwritePartitionsWithAddedColumns
      )

      // checking pre-conditions
      sourceTable.read().count() shouldBe 19
      targetTable.read().count() shouldBe 1
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=2/day=15")) shouldBe
        true

      // execute load
      Materialization.newQueryMaterialization(spark, dfs, paramsFileHdfsPath.toUri.toString).run()

      // compare data
      val actualDf = targetTable.read()
      val expectedDf = fileReader.read(spark, getExpectedDataFile(testDir))
      expectedDf.hasDiff(actualDf) shouldBe false

      // check that the partition exists
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=2/day=15")) shouldBe
        true
    }

    Scenario("When other partitions exist on the filesystem") {
      val testDir = "query_materialization"
      prepareTables(testDir, Seq("year", "month", "day"))
      copyResourceFileToHdfs(s"$testDir/$paramsFileName", paramsFileHdfsPath)

      // adding data to target table
      targetTable.write(
        Seq(getInitialDataFile(testDir)),
        fileReader,
        LoadMode.OverwritePartitionsWithAddedColumns
      )

      // check pre-conditions
      sourceTable.read().count() shouldBe 19
      targetTable.read().count() shouldBe 2
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=3/day=15")) shouldBe
        true
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=6/day=15")) shouldBe
        true

      // execute load
      Materialization.newQueryMaterialization(spark, dfs, paramsFileHdfsPath.toUri.toString).run()

      // compare data
      val actualDf = targetTable.read()
      val newPartitionDf = fileReader.read(spark, getExpectedDataFile(testDir))
      val intermediatePartitionsDf = fileReader.read(spark, getInitialDataFile(testDir))
      val expectedDf = newPartitionDf.union(intermediatePartitionsDf)
      actualDf.hasDiff(expectedDf) shouldBe false

      // check that new partition was created
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=2/day=15")) shouldBe
        true
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=3/day=15")) shouldBe
        true
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=6/day=15")) shouldBe
        true
    }
  }

  Feature("The number of output partitions should be configurable") {
    Scenario("When number of output partitions is 5") {
      val testDir = "query_materialization"
      prepareTables(testDir, Seq("year", "month", "day"))
      copyResourceFileToHdfs(s"$testDir/output_files_5/$paramsFileName", paramsFileHdfsPath)

      // adding data to target table
      targetTable.write(
        Seq(getInitialDataFile(testDir)),
        fileReader,
        LoadMode.OverwritePartitionsWithAddedColumns
      )

      // check pre-conditions
      sourceTable.read().count() shouldBe 19
      targetTable.read().count() shouldBe 2
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=3/day=15")) shouldBe
        true
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=6/day=15")) shouldBe
        true

      // execute load
      Materialization.newQueryMaterialization(spark, dfs, paramsFileHdfsPath.toUri.toString).run()

      // compare data
      val actualDf = targetTable.read()
      val newPartitionDf = fileReader.read(spark, getExpectedDataFile(testDir))
      val intermediatePartitionsDf = fileReader.read(spark, getInitialDataFile(testDir))
      val expectedDf = newPartitionDf.union(intermediatePartitionsDf)
      actualDf.hasDiff(expectedDf) shouldBe false

      // check that new partition was created
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=2/day=15")) shouldBe
        true
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=3/day=15")) shouldBe
        true
      fs.exists(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=6/day=15")) shouldBe
        true
      fs.listStatus(new Path(hdfsRootTestPath, "target_table/data/year=2017/month=2/day=15"))
        .length shouldBe 5
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.sql(s"DROP DATABASE IF EXISTS $targetDatabase CASCADE")
    spark.sql(s"DROP DATABASE IF EXISTS $sourceDatabase CASCADE")
    spark.sql(s"CREATE DATABASE $sourceDatabase")
    spark.sql(s"CREATE DATABASE $targetDatabase")
  }

  private def getSourceDataFile(testDir: String): String =
    resolveResource(s"$testDir/source_data.psv", withProtocol = true)

  private def getInitialDataFile(testDir: String): String =
    resolveResource(s"$testDir/initial_data.psv", withProtocol = true)

  private def getExpectedDataFile(testDir: String): String =
    resolveResource(s"$testDir/expected_data.psv", withProtocol = true)

  private def prepareTables(testDir: String, partitions: Seq[String]): Unit = {
    val schema =
      DataType.fromJson(getResourceAsText(s"$testDir/schema.json")).asInstanceOf[StructType]

    // create file reader with the current schema
    fileReader = FileReader.newDSVFileReader(optionalSchema = Some(schema))

    // create source table and add data to it
    sourceTable = createTable("source_table", sourceDatabase, partitions, "source_table", schema)
    sourceTable.write(
      Seq(getSourceDataFile(testDir)),
      fileReader,
      LoadMode.OverwritePartitionsWithAddedColumns
    )

    // create target table
    targetTable = createTable("target_table", targetDatabase, partitions, "target_table", schema)
  }

  private def createTable(
      table: String,
      database: String,
      partitions: Seq[String],
      tableDirName: String,
      schema: StructType
  ): Table =
    Table
      .newBuilder(
        table,
        database,
        fs.makeQualified(new Path(hdfsRootTestPath, s"$tableDirName/data")).toString,
        schema
      )
      .withPartitions(partitions)
      .buildParquetTable(DFSWrapper(fs.getConf), spark, external = true)

  /** Creates folders and files in the filesystem from previous materialization attempts, also
    * including some leftovers from executions of other versions of the algorithm that did not use
    * the timestamped folder structure.
    *
    * @param tableDataPath
    *   Path to table's base data dir (e.g., "data/")
    * @return
    *   previous versions and leftover files locations
    */
  private def createPreviousVersionsAndLeftovers(tableDataPath: Path): Seq[Path] = {
    val leftoverParquetFiles = Seq[Path](
      new Path(tableDataPath, "part-0001.parquet"),
      new Path(tableDataPath, "part-0002.parquet"),
      new Path(tableDataPath, "part-0003.parquet"),
      new Path(tableDataPath, "part-0004.parquet")
    )

    leftoverParquetFiles.foreach(location => fs.createNewFile(location))

    val tableLocations = Seq[Path](
      new Path(tableDataPath, "year=2019/"),
      new Path(tableDataPath, "year=2020/"),
      new Path(tableDataPath, "20200101_124514_UTC/"),
      new Path(tableDataPath, "20200102_123012_UTC/"),
      new Path(tableDataPath, "20200103_114329_UTC/"),
      new Path(tableDataPath, "20200201_103210_UTC/")
    )

    tableLocations.foreach { location =>
      fs.mkdirs(location)
      fs.createNewFile(new Path(tableDataPath, s"${location.getName}_$$folder$$"))
      fs.createNewFile(new Path(location, "sample_file.parquet"))
    }

    leftoverParquetFiles ++ tableLocations
  }
}
