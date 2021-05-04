package com.adidas.analytics.feature.loads

import com.adidas.analytics.algo.loads.FullLoad
import com.adidas.analytics.util.{DFSWrapper, HadoopLoadHelper, CatalogTableManager, LoadMode}
import com.adidas.utils.TestUtils._
import com.adidas.utils.{BaseAlgorithmTest, FileReader, Table}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers._

class FullLoadTest extends AnyFeatureSpec with BaseAlgorithmTest {

  private val sourceEnvironmentLocation: String = "test_landing"
  private val targetDatabase: String = "test_lake"
  private val tableName: String = "test_table"

  private val paramsFileName: String = "params.json"

  private val paramsFileHdfsPath: Path = new Path(hdfsRootTestPath, paramsFileName)

  private val sourceDirPath: Path =
    new Path(hdfsRootTestPath, s"$sourceEnvironmentLocation/test/$tableName/data")

  private val baseTargetDirPath: Path =
    new Path(hdfsRootTestPath, s"$targetDatabase/test/$tableName/data")

  Feature("Reader mode can be specified in configuration") {
    Scenario("when reader_mode is invalid string an exception is thrown") {
      val resourceDir = "failfast_option"
      copyResourceFileToHdfs(s"$resourceDir/params_invalid_reader_mode.json", paramsFileHdfsPath)

      val targetSchema = DataType
        .fromJson(getResourceAsText(s"$resourceDir/target_schema.json"))
        .asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))

      val targetTable = createNonPartitionedTargetTable(targetSchema)
      setupInitialState(targetTable, s"$resourceDir/lake_data_pre.psv", dataReader)
      prepareSourceData(Seq(s"$resourceDir/new_data_wrong.psv"))

      // checking pre-conditions
      spark.read.csv(sourceDirPath.toString).count() shouldBe 25
      targetTable.read().count() shouldBe 19

      val caught =
        intercept[RuntimeException](FullLoad(spark, dfs, paramsFileHdfsPath.toString).run())
      caught.getMessage shouldBe "Invalid reader mode: invalid_mode provided"
    }

    Scenario(
      "when reader mode is FailFast and malformed records are present, an exception is thrown"
    ) {
      val resourceDir = "failfast_option"
      copyResourceFileToHdfs(s"$resourceDir/params.json", paramsFileHdfsPath)

      val targetSchema = DataType
        .fromJson(getResourceAsText(s"$resourceDir/target_schema.json"))
        .asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))

      val targetTable = createNonPartitionedTargetTable(targetSchema)
      setupInitialState(targetTable, s"$resourceDir/lake_data_pre.psv", dataReader)
      prepareSourceData(Seq(s"$resourceDir/new_data_wrong.psv"))

      // checking pre-conditions
      spark.read.csv(sourceDirPath.toString).count() shouldBe 25
      targetTable.read().count() shouldBe 19

      val caught =
        intercept[RuntimeException](FullLoad(spark, dfs, paramsFileHdfsPath.toString).run())
      caught.getMessage shouldBe "Unable to write DataFrames."
    }

    Scenario(
      "when reader mode is FailFast and no malformed records are present, load is completed correctly"
    ) {
      val resourceDir = "failfast_option"
      copyResourceFileToHdfs(s"$resourceDir/params.json", paramsFileHdfsPath)

      val targetSchema = DataType
        .fromJson(getResourceAsText(s"$resourceDir/target_schema.json"))
        .asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))

      val targetTable = createNonPartitionedTargetTable(targetSchema)
      setupInitialState(targetTable, s"$resourceDir/lake_data_pre.psv", dataReader)
      prepareDefaultSourceData()

      // checking pre-conditions
      spark.read.csv(sourceDirPath.toString).count() shouldBe 25
      targetTable.read().count() shouldBe 19

      FullLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      // validating result
      val expectedDataLocation =
        resolveResource(s"$resourceDir/lake_data_post.psv", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = targetTable.read()
      actualDf.hasDiff(expectedDf) shouldBe false
    }

    Scenario(
      "when reader mode is DROPMALFORMED and malformed records are present, some records are not loaded"
    ) {
      val resourceDir = "failfast_option"
      copyResourceFileToHdfs(s"$resourceDir/params_dropmalformed_mode.json", paramsFileHdfsPath)

      val targetSchema = DataType
        .fromJson(getResourceAsText(s"$resourceDir/target_schema.json"))
        .asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))

      val targetTable = createNonPartitionedTargetTable(targetSchema)
      setupInitialState(targetTable, s"$resourceDir/lake_data_pre.psv", dataReader)
      prepareSourceData(Seq(s"$resourceDir/new_data_wrong.psv"))

      // checking pre-conditions
      spark.read.csv(sourceDirPath.toString).count() shouldBe 25
      targetTable.read().count() shouldBe 19

      FullLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      // validating result
      val expectedDataLocation =
        resolveResource(s"$resourceDir/lake_data_post.psv", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = targetTable.read()
      assert(actualDf.count() < expectedDf.count())
    }

    Scenario(
      "when reader mode is PERMISSIVE and malformed records are present, malformed records are also loaded"
    ) {
      val resourceDir = "failfast_option"
      copyResourceFileToHdfs(s"$resourceDir/params_permissive_mode.json", paramsFileHdfsPath)

      val targetSchema = DataType
        .fromJson(getResourceAsText(s"$resourceDir/target_schema.json"))
        .asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))

      val targetTable = createNonPartitionedTargetTable(targetSchema)
      setupInitialState(targetTable, s"$resourceDir/lake_data_pre.psv", dataReader)
      prepareSourceData(Seq(s"$resourceDir/new_data_wrong.psv"))

      // checking pre-conditions
      spark.read.csv(sourceDirPath.toString).count() shouldBe 25
      targetTable.read().count() shouldBe 19

      FullLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      // validating result
      val expectedDataLocation =
        resolveResource(s"$resourceDir/lake_data_post.psv", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = targetTable.read()
      actualDf.hasDiff(expectedDf) shouldBe true
      actualDf.count() shouldBe expectedDf.count()
    }
  }

  Feature("Data can be loaded from source to target with full mode") {
    Scenario("Previous lake table location folder does not exist.") {
      val resourceDir = "non_partitioned"
      copyResourceFileToHdfs(s"$resourceDir/$paramsFileName", paramsFileHdfsPath)

      val targetSchema = DataType
        .fromJson(getResourceAsText(s"$resourceDir/target_schema.json"))
        .asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))

      val targetTable = createNonPartitionedTargetTable(targetSchema)
      setupInitialState(targetTable, s"$resourceDir/lake_data_pre.psv", dataReader)
      prepareDefaultSourceData()

      // checking pre-conditions
      spark.read.csv(sourceDirPath.toString).count() shouldBe 25
      targetTable.read().count() shouldBe 19

      // Deleting table location folder before full load
      fs.delete(new Path(targetTable.location), true)

      FullLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      // validating result
      val expectedDataLocation =
        resolveResource(s"$resourceDir/lake_data_post.psv", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = targetTable.read()
      actualDf.hasDiff(expectedDf) shouldBe false

      // validating schema
      val expectedTS = targetSchema
      val actualTS = spark.table(targetDatabase + '.' + tableName).schema
      actualTS.equals(expectedTS) shouldBe true
    }

    Scenario("Loading data to non-partitioned table") {
      val resourceDir = "non_partitioned"
      copyResourceFileToHdfs(s"$resourceDir/$paramsFileName", paramsFileHdfsPath)

      val targetSchema = DataType
        .fromJson(getResourceAsText(s"$resourceDir/target_schema.json"))
        .asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))

      val targetTable = createNonPartitionedTargetTable(targetSchema)
      setupInitialState(targetTable, s"$resourceDir/lake_data_pre.psv", dataReader)
      prepareDefaultSourceData()

      // checking pre-conditions
      spark.read.csv(sourceDirPath.toString).count() shouldBe 25
      targetTable.read().count() shouldBe 19

      FullLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      // validating result
      val expectedDataLocation =
        resolveResource(s"$resourceDir/lake_data_post.psv", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = targetTable.read()
      actualDf.hasDiff(expectedDf) shouldBe false
    }

    Scenario("Loading data to partitioned table") {
      val resourceDir = "partitioned"
      copyResourceFileToHdfs(s"$resourceDir/$paramsFileName", paramsFileHdfsPath)

      val targetSchema = DataType
        .fromJson(getResourceAsText(s"$resourceDir/target_schema.json"))
        .asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))

      val targetTable =
        createPartitionedTargetTable(Seq("year", "month", "day"), targetSchema, tableName)
      var targetPath20180110 = new Path(targetTable.location, "year=2018/month=1/day=10")
      setupInitialState(targetTable, s"$resourceDir/lake_data_pre.psv", dataReader)
      prepareDefaultSourceData()

      // checking pre-conditions
      spark.read.csv(sourceDirPath.toString).count() shouldBe 25
      targetTable.read().count() shouldBe 19
      fs.exists(targetPath20180110) shouldBe false

      // executing load
      FullLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      // validating result
      val expectedDataLocation =
        resolveResource(s"$resourceDir/lake_data_post.psv", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = targetTable.read()
      actualDf.hasDiff(expectedDf) shouldBe false

      targetPath20180110 = new Path(
        CatalogTableManager(targetTable.table, spark).getTableLocation,
        "year=2018/month=1/day=10"
      )
      fs.exists(targetPath20180110) shouldBe true
    }
    Scenario("Loading data to table partitioned by multiple non-derived columns") {
      val resourceDir = "partitioned_multi_columns"
      copyResourceFileToHdfs(s"$resourceDir/$paramsFileName", paramsFileHdfsPath)

      val targetSchema = DataType
        .fromJson(getResourceAsText(s"$resourceDir/target_schema.json"))
        .asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))

      val targetTable =
        createPartitionedTargetTable(Seq("customer", "date"), targetSchema, tableName)
      var targetPathTestPartition =
        new Path(targetTable.location, "customer=customer5/date=20180110")
      setupInitialState(targetTable, s"$resourceDir/lake_data_pre.psv", dataReader)
      prepareDefaultSourceData("landing/new_data_multi_partition_columns.psv")

      // checking pre-conditions
      spark.read.csv(sourceDirPath.toString).count() shouldBe 25
      targetTable.read().count() shouldBe 19
      fs.exists(targetPathTestPartition) shouldBe false

      // executing load
      FullLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      // validating result
      val expectedDataLocation =
        resolveResource(s"$resourceDir/lake_data_post.psv", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = targetTable.read()
      actualDf.hasDiff(expectedDf) shouldBe false

      targetPathTestPartition = new Path(
        CatalogTableManager(targetTable.table, spark).getTableLocation,
        "customer=customer5/date=20180110"
      )
      fs.exists(targetPathTestPartition) shouldBe true
    }

    Scenario(
      "Partitioned table is loaded and old leftovers are cleansed properly after successful load"
    ) {
      val resourceDir = "partitioned"
      copyResourceFileToHdfs(s"$resourceDir/$paramsFileName", paramsFileHdfsPath)

      val targetSchema = DataType
        .fromJson(getResourceAsText(s"$resourceDir/target_schema.json"))
        .asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))

      val targetTable =
        createPartitionedTargetTable(Seq("year", "month", "day"), targetSchema, tableName)
      var targetPath20180110 = new Path(targetTable.location, "year=2018/month=1/day=10")
      setupInitialState(targetTable, s"$resourceDir/lake_data_pre.psv", dataReader)
      prepareDefaultSourceData()

      //manually create old leftovers
      val tableRootPath = new Path(hdfsRootTestPath, s"$targetDatabase/test/$tableName")
      val oldTableLocation1 = new Path(tableRootPath, "data_20000101124514567/year=2000")
      val oldTableLocation2 = new Path(tableRootPath, "data_20000221124511234/year=2000")
      fs.mkdirs(oldTableLocation1)
      fs.mkdirs(oldTableLocation2)
      fs.createNewFile(new Path(tableRootPath, "data_20000101124514567_$folder$"))
      fs.createNewFile(new Path(tableRootPath, "data_20000221124511234_$folder$"))
      fs.createNewFile(new Path(oldTableLocation1, "sample_file1.parquet"))
      fs.createNewFile(new Path(oldTableLocation1, "sample_file2.parquet"))
      fs.createNewFile(new Path(oldTableLocation2, "sample_file1.parquet"))
      fs.createNewFile(new Path(oldTableLocation2, "sample_file2.parquet"))

      /* num of files in table's root dir should be the leftovers plus the initial state folder */
      var numFilesInTableRootDir = fs.listStatus(tableRootPath).count(_ => true)
      numFilesInTableRootDir shouldBe 5

      // checking pre-conditions
      spark.read.csv(sourceDirPath.toString).count() shouldBe 25
      targetTable.read().count() shouldBe 19
      fs.exists(targetPath20180110) shouldBe false

      // executing load
      FullLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      // validating result
      val expectedDataLocation =
        resolveResource(s"$resourceDir/lake_data_post.psv", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = targetTable.read()
      actualDf.hasDiff(expectedDf) shouldBe false

      val finalTableLocation = CatalogTableManager(targetTable.table, spark).getTableLocation
      targetPath20180110 = new Path(finalTableLocation, "year=2018/month=1/day=10")
      fs.exists(targetPath20180110) shouldBe true

      // table root folder only contains the final table location
      val fileStatus = fs.listStatus(tableRootPath)
      numFilesInTableRootDir = fileStatus.count(_ => true)
      numFilesInTableRootDir shouldBe 1

      /* most recent subfolder is the table location and its parent folder is as expected */
      val mostRecentSubFolder = fileStatus.toList.head.getPath
      mostRecentSubFolder.getParent.getName shouldBe tableRootPath.getName
      (mostRecentSubFolder.toString == finalTableLocation) shouldBe true
    }

    Scenario("Loading data to partitioned table in weekly mode") {
      val resourceDir = "partitioned_weekly"
      copyResourceFileToHdfs(s"$resourceDir/$paramsFileName", paramsFileHdfsPath)

      val targetSchema = DataType
        .fromJson(getResourceAsText(s"$resourceDir/target_schema.json"))
        .asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))

      val targetTable = createPartitionedTargetTable(Seq("year", "week"), targetSchema, tableName)
      var targetPath201801 = new Path(targetTable.location, "year=2018/week=1")
      setupInitialState(targetTable, s"$resourceDir/lake_data_pre.psv", dataReader)
      prepareDefaultSourceData("landing/new_data_weekly.psv")

      // checking pre-conditions
      spark.read.csv(sourceDirPath.toString).count() shouldBe 25
      targetTable.read().count() shouldBe 19

      fs.exists(targetPath201801) shouldBe false

      // executing load
      FullLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      // validating result
      val expectedDataLocation =
        resolveResource(s"$resourceDir/lake_data_post.psv", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = targetTable.read()
      actualDf.hasDiff(expectedDf) shouldBe false

      targetPath201801 =
        new Path(CatalogTableManager(targetTable.table, spark).getTableLocation, "year=2018/week=1")
      fs.exists(targetPath201801) shouldBe true
    }

    Scenario(
      "Try loading data from location that does not exist and expect the data to be as it was before load"
    ) {
      val resourceDir = "partitioned"
      copyResourceFileToHdfs(s"partitioned_not_exist_dir/$paramsFileName", paramsFileHdfsPath)

      val targetSchema = DataType
        .fromJson(getResourceAsText(s"$resourceDir/target_schema.json"))
        .asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))

      val targetTable =
        createPartitionedTargetTable(Seq("year", "month", "day"), targetSchema, tableName)
      val targetPath20180110 = new Path(targetTable.location, "year=2018/month=1/day=10")
      setupInitialState(targetTable, s"$resourceDir/lake_data_pre.psv", dataReader)

      targetTable.read().count() shouldBe 19

      // executing load
      val caught =
        intercept[RuntimeException](FullLoad(spark, dfs, paramsFileHdfsPath.toString).run())

      assert(caught.getMessage.equals("Unable to read input data."))

      // validating result
      val expectedDataLocation =
        resolveResource(s"$resourceDir/lake_data_pre.psv", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = targetTable.read()
      actualDf.hasDiff(expectedDf) shouldBe false

      fs.exists(targetPath20180110) shouldBe false
    }

    Scenario("Try loading data while partitioning column is missing") {
      val resourceDir = "partitioned"
      copyResourceFileToHdfs(
        s"partitioned_partition_column_wrong/$paramsFileName",
        paramsFileHdfsPath
      )

      val targetSchema = DataType
        .fromJson(getResourceAsText(s"$resourceDir/target_schema.json"))
        .asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))

      val targetTable =
        createPartitionedTargetTable(Seq("year", "month", "day"), targetSchema, tableName)
      val targetPath20180110 = new Path(targetTable.location, "year=2018/month=1/day=10")
      setupInitialState(targetTable, s"$resourceDir/lake_data_pre.psv", dataReader)
      prepareDefaultSourceData()

      // checking pre-conditions
      targetTable.read().count() shouldBe 19
      fs.exists(targetPath20180110) shouldBe false

      // executing load
      val caught =
        intercept[RuntimeException](FullLoad(spark, dfs, paramsFileHdfsPath.toString).run())

      assert(caught.getMessage.equals("Unable to transform data frames."))

      // validating result
      val expectedDataLocation =
        resolveResource(s"$resourceDir/lake_data_pre.psv", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = targetTable.read()

      actualDf.hasDiff(expectedDf) shouldBe false

      fs.exists(targetPath20180110) shouldBe false
    }

    Scenario("Try loading data while date format is wrong") {
      val resourceDir = "partitioned_date_format_wrong"
      copyResourceFileToHdfs(s"$resourceDir/$paramsFileName", paramsFileHdfsPath)

      val targetSchema = DataType
        .fromJson(getResourceAsText(s"$resourceDir/target_schema.json"))
        .asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))

      val targetTable =
        createPartitionedTargetTable(Seq("year", "month", "day"), targetSchema, tableName)
      var targetPath99999999 = new Path(targetTable.location, "year=9999/month=99/day=99")
      setupInitialState(targetTable, s"$resourceDir/lake_data_pre.psv", dataReader)
      prepareDefaultSourceData()

      // checking pre-conditions
      targetTable.read().count() shouldBe 19
      fs.exists(targetPath99999999) shouldBe false

      // executing load
      FullLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      // validating result
      val expectedDataLocation =
        resolveResource(s"$resourceDir/lake_data_post.psv", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = targetTable.read()
      actualDf.hasDiff(expectedDf) shouldBe false

      targetPath99999999 = new Path(
        CatalogTableManager(targetTable.table, spark).getTableLocation,
        "year=9999/month=99/day=99"
      )
      fs.exists(targetPath99999999) shouldBe true
    }
  }

  Feature("Full load with nested flattener job") {
    Scenario("Full load nested with nested flattener and transpose") {
      val resourceDir = "nested_flattener"

      val paramsFileNameExtended: String = "params_transpose_scenario.json"

      val paramsFileHdfsPath: Path = new Path(hdfsRootTestPath, paramsFileNameExtended)

      copyResourceFileToHdfs(s"$resourceDir/$paramsFileNameExtended", paramsFileHdfsPath)

      val targetSchema =
        DataType
          .fromJson(getResourceAsText(s"$resourceDir/target_schema_transpose_scenario.json"))
          .asInstanceOf[StructType]

      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))

      val targetTable = createNonPartitionedTargetTable(targetSchema)

      setupInitialState(targetTable, s"$resourceDir/data_transpose_test.json", dataReader)
      prepareDefaultSourceData("nested_flattener/data_transpose_test.json")

      FullLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      // validating result
      val expectedDataLocation =
        resolveResource(s"$resourceDir/expected_target_data_tranpose.psv", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)

      val actualDf = targetTable.read()
      actualDf.hasDiff(expectedDf) shouldBe false
    }

    Scenario("Full load nested with nested flattener only") {
      val resourceDir = "nested_flattener"

      val paramsFileNameExtended: String = "params_normal_scenario.json"
      val paramsFileHdfsPath: Path = new Path(hdfsRootTestPath, paramsFileNameExtended)

      copyResourceFileToHdfs(s"$resourceDir/$paramsFileNameExtended", paramsFileHdfsPath)

      val targetSchema =
        DataType
          .fromJson(getResourceAsText(s"$resourceDir/target_schema_extend.json"))
          .asInstanceOf[StructType]

      copyResourceFileToHdfs(s"$resourceDir/$paramsFileNameExtended", paramsFileHdfsPath)

      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))

      val targetTable = createNonPartitionedTargetTable(targetSchema)

      setupInitialState(targetTable, s"$resourceDir/data_normal_test.json", dataReader)
      prepareDefaultSourceData("nested_flattener/data_normal_test.json")

      // checking pre-conditions
      spark.read.json(sourceDirPath.toString).count() shouldBe 1
      targetTable.read().count() shouldBe 1

      FullLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      // validating result
      val expectedDataLocation =
        resolveResource(s"$resourceDir/expected_target_data_extend.psv", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)

      val actualDf = targetTable.read()
      actualDf.hasDiff(expectedDf) shouldBe false
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.sql(s"DROP DATABASE IF EXISTS $targetDatabase CASCADE")
    spark.sql(s"CREATE DATABASE $targetDatabase")
    logger.info(s"Creating ${sourceDirPath.toString}")
    fs.mkdirs(sourceDirPath)
  }

  private def createPartitionedTargetTable(
      targetPartitions: Seq[String],
      targetSchema: StructType,
      tableName: String
  ): Table = {
    val targetTableLocation = fs
      .makeQualified(
        new Path(hdfsRootTestPath, HadoopLoadHelper.buildTimestampedTablePath(baseTargetDirPath))
      )
      .toString
    Table
      .newBuilder(tableName, targetDatabase, targetTableLocation, targetSchema)
      .withPartitions(targetPartitions)
      .buildParquetTable(DFSWrapper(fs.getConf), spark, external = true)
  }

  private def createNonPartitionedTargetTable(targetSchema: StructType): Table =
    createNonPartitionedTargetTable(
      targetSchema,
      HadoopLoadHelper.buildTimestampedTablePath(baseTargetDirPath)
    )

  private def createNonPartitionedTargetTable(targetSchema: StructType, dir: Path): Table = {
    val targetTableLocation = fs.makeQualified(new Path(hdfsRootTestPath, dir)).toString
    Table
      .newBuilder(tableName, targetDatabase, targetTableLocation, targetSchema)
      .buildParquetTable(DFSWrapper(fs.getConf), spark, external = true)
  }

  private def prepareDefaultSourceData(sourceData: String = "landing/new_data.psv"): Unit =
    prepareSourceData(Seq(sourceData))

  private def prepareSourceData(sourceFiles: Seq[String]): Unit =
    sourceFiles.foreach { file =>
      logger.info(s"copyResourceFileToHdfs $file to ${sourceDirPath.toString}")
      copyResourceFileToHdfs(s"$file", sourceDirPath)
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
}
