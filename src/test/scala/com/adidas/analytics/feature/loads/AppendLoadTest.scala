package com.adidas.analytics.feature.loads

import com.adidas.analytics.algo.loads.AppendLoad
import com.adidas.analytics.util.DFSWrapper._
import com.adidas.analytics.util.{DFSWrapper, LoadMode}
import com.adidas.utils.TestUtils._
import com.adidas.utils.{BaseAlgorithmTest, FileReader, Table}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkException
import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers._

class AppendLoadTest extends AnyFeatureSpec with BaseAlgorithmTest {

  private val sourceDatabase: String = "test_landing"
  private val targetDatabase: String = "test_lake"
  private val tableName: String = "test_table"

  private val paramsFileName: String = "params.json"

  private val paramsFileHdfsPath: Path = new Path(hdfsRootTestPath, paramsFileName)

  private val sourceDirPath: Path = new Path(hdfsRootTestPath, s"$sourceDatabase/$tableName/data")

  private val headerDirPath: Path = new Path(hdfsRootTestPath, s"$sourceDatabase/$tableName/header")

  private val targetDirPath: Path = new Path(hdfsRootTestPath, s"$targetDatabase/$tableName")

  Feature("Reader mode can be specified in configuration") {
    Scenario("when reader_mode is set to an invalid string, an exception is thrown") {
      val testResourceDir = "reader_mode_specification"
      val headerPath20180422 = new Path(headerDirPath, "year=2018/month=4/day=22/header.json")
      val targetPath20180422 = new Path(targetDirPath, "year=2018/month=4/day=22")

      val targetSchema =
        DataType
          .fromJson(getResourceAsText(s"$testResourceDir/target_schema.json"))
          .asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))

      val targetTable = createTargetTable(Seq("year", "month", "day"), targetSchema)
      prepareSourceData(testResourceDir, Seq("wrong_data_20180422-00001.psv"))
      setupInitialState(targetTable, s"$testResourceDir/lake_data_pre.psv", dataReader)
      uploadParameters(testResourceDir, "params_invalid_reader_mode.json")

      // checking pre-conditions
      spark.read.option("header", "true").csv(sourceDirPath.toString).count() shouldBe 8
      targetTable.read().count() shouldBe 19

      fs.exists(targetPath20180422) shouldBe false
      fs.exists(headerPath20180422) shouldBe false

      // executing load
      val caught =
        intercept[RuntimeException](AppendLoad(spark, dfs, paramsFileHdfsPath.toString).run())

      caught.getMessage shouldBe "Invalid reader mode: invalid_mode provided"
    }

    Scenario("when reader mode is not set, DROPMALFORMED is used") {
      val testResourceDir = "reader_mode_specification"
      val headerPath20180422 = new Path(headerDirPath, "year=2018/month=4/day=22/header.json")
      val targetPath20180422 = new Path(targetDirPath, "year=2018/month=4/day=22")

      val targetSchema =
        DataType
          .fromJson(getResourceAsText(s"$testResourceDir/target_schema.json"))
          .asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))

      val targetTable = createTargetTable(Seq("year", "month", "day"), targetSchema)
      prepareSourceData(testResourceDir, Seq("wrong_data_20180422-00001.psv"))
      setupInitialState(targetTable, s"$testResourceDir/lake_data_pre.psv", dataReader)
      uploadParameters(testResourceDir, "params_no_reader_mode.json")

      // checking pre-conditions
      spark.read.option("header", "true").csv(sourceDirPath.toString).count() shouldBe 8
      targetTable.read().count() shouldBe 19

      fs.exists(targetPath20180422) shouldBe false
      fs.exists(headerPath20180422) shouldBe false

      // executing load
      AppendLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      // validating result
      val expectedDataLocation =
        resolveResource(s"$testResourceDir/lake_data_post.psv", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = targetTable.read()
      actualDf.hasDiff(expectedDf) shouldBe true

      fs.exists(targetPath20180422) shouldBe true
      fs.exists(headerPath20180422) shouldBe true
    }

    Scenario(
      "when reader mode is set to FAILFAST and there are malformed records, an exception is thrown"
    ) {
      val testResourceDir = "reader_mode_specification"
      val headerPath20180422 = new Path(headerDirPath, "year=2018/month=4/day=22/header.json")
      val targetPath20180422 = new Path(targetDirPath, "year=2018/month=4/day=22")

      val targetSchema =
        DataType
          .fromJson(getResourceAsText(s"$testResourceDir/target_schema.json"))
          .asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))

      val targetTable = createTargetTable(Seq("year", "month", "day"), targetSchema)
      prepareSourceData(testResourceDir, Seq("wrong_data_20180422-00001.psv"))
      setupInitialState(targetTable, s"$testResourceDir/lake_data_pre.psv", dataReader)
      uploadParameters(testResourceDir, "params_failfast_mode.json")

      // checking pre-conditions
      spark.read.option("header", "true").csv(sourceDirPath.toString).count() shouldBe 8
      targetTable.read().count() shouldBe 19

      fs.exists(targetPath20180422) shouldBe false
      fs.exists(headerPath20180422) shouldBe false

      // executing load
      assertThrows[SparkException](AppendLoad(spark, dfs, paramsFileHdfsPath.toString).run())
    }

    Scenario(
      "when reader mode is set to PERMISSIVE and there are malformed records, data is incorrectly loaded"
    ) {
      val testResourceDir = "reader_mode_specification"
      val headerPath20180422 = new Path(headerDirPath, "year=2018/month=4/day=22/header.json")
      val targetPath20180422 = new Path(targetDirPath, "year=2018/month=4/day=22")

      val targetSchema =
        DataType
          .fromJson(getResourceAsText(s"$testResourceDir/target_schema.json"))
          .asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))

      val targetTable = createTargetTable(Seq("year", "month", "day"), targetSchema)
      prepareSourceData(testResourceDir, Seq("wrong_data_20180422-00001.psv"))
      setupInitialState(targetTable, s"$testResourceDir/lake_data_pre.psv", dataReader)
      uploadParameters(testResourceDir, "params_permissive_mode.json")

      // checking pre-conditions
      spark.read.option("header", "true").csv(sourceDirPath.toString).count() shouldBe 8
      targetTable.read().count() shouldBe 19

      fs.exists(targetPath20180422) shouldBe false
      fs.exists(headerPath20180422) shouldBe false

      // executing load
      AppendLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      // validating result
      val expectedDataLocation =
        resolveResource(s"$testResourceDir/lake_data_post.psv", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = targetTable.read()
      actualDf.hasDiff(expectedDf) shouldBe true
    }
  }

  Feature("Data can be loaded from source to target with append mode") {
    Scenario("Data can be loaded with append mode by creating partitions from full path") {
      val tableNamePartFromFullPath: String = "test_table_full_path_part"
      val paramsFileModdedRegexHdfsPath: Path = new Path(hdfsRootTestPath, paramsFileName)
      val sourceDirFullPath: Path = new Path(
        hdfsRootTestPath,
        s"$sourceDatabase/$tableNamePartFromFullPath/data/year=2018/month=01/day=01/"
      )
      val headerDirPathPartFromFullPath: Path =
        new Path(hdfsRootTestPath, s"$sourceDatabase/$tableNamePartFromFullPath/header")
      val targetDirPathPartFromFullPath: Path =
        new Path(hdfsRootTestPath, s"$targetDatabase/$tableNamePartFromFullPath")

      fs.mkdirs(sourceDirFullPath)
      fs.mkdirs(headerDirPathPartFromFullPath)
      fs.mkdirs(targetDirPathPartFromFullPath)

      val testResourceDir = "partition_from_full_path"
      val headerPath20180101 =
        new Path(headerDirPathPartFromFullPath, "year=2018/month=1/day=1/header.json")
      val targetPath20180101 = new Path(targetDirPathPartFromFullPath, "year=2018/month=1/day=1")

      val targetSchema =
        DataType
          .fromJson(getResourceAsText(s"$testResourceDir/target_schema.json"))
          .asInstanceOf[StructType]

      val targetTableLocation =
        fs.makeQualified(new Path(hdfsRootTestPath, targetDirPathPartFromFullPath)).toString
      val targetTable = Table
        .newBuilder(tableNamePartFromFullPath, targetDatabase, targetTableLocation, targetSchema)
        .withPartitions(Seq("year", "month", "day"))
        .buildParquetTable(DFSWrapper(fs.getConf), spark, external = true)

      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))
      setupInitialState(targetTable, s"$testResourceDir/lake_data_pre.psv", dataReader)
      prepareSourceData(
        testResourceDir,
        Seq("data-nodate-part-00000.psv", "data-nodate-part-00001.psv"),
        sourceDirFullPath
      )
      uploadParameters(testResourceDir, paramsFileName, paramsFileModdedRegexHdfsPath)

      // checking pre-conditions
      spark.read.csv(sourceDirFullPath.toString).count() shouldBe 7
      targetTable.read().count() shouldBe 19

      fs.exists(targetPath20180101) shouldBe false
      fs.exists(headerPath20180101) shouldBe false

      // executing load
      AppendLoad(spark, dfs, paramsFileModdedRegexHdfsPath.toString).run()

      // validating result
      val expectedDataLocation =
        resolveResource(s"$testResourceDir/lake_data_post.psv", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = targetTable.read()
      actualDf.hasDiff(expectedDf) shouldBe false

      fs.exists(targetPath20180101) shouldBe true
      fs.exists(headerPath20180101) shouldBe true
    }

    Scenario("Loading data from multiple files") {
      val testResourceDir = "multiple_source_files"
      val headerPath20180101 = new Path(headerDirPath, "year=2018/month=1/day=1/header.json")
      val targetPath20180101 = new Path(targetDirPath, "year=2018/month=1/day=1")

      val targetSchema =
        DataType
          .fromJson(getResourceAsText(s"$testResourceDir/target_schema.json"))
          .asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))

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
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = targetTable.read()
      actualDf.hasDiff(expectedDf) shouldBe false

      fs.exists(targetPath20180101) shouldBe true
      fs.exists(headerPath20180101) shouldBe true
    }

    Scenario(
      "Loading data from hierarchical directory structure and one of the partitions has the only bad record"
    ) {
      val testResourceDir = "hierarchical_load"

      val headerPath20180101 = new Path(headerDirPath, "year=2018/month=1/day=1/header.json")
      val headerPath20180105 = new Path(headerDirPath, "year=2018/month=1/day=5/header.json")
      val targetPath20180101 = new Path(targetDirPath, "year=2018/month=1/day=1")
      val targetPath20180105 = new Path(targetDirPath, "year=2018/month=1/day=5")

      val targetSchema =
        DataType
          .fromJson(getResourceAsText(s"$testResourceDir/target_schema.json"))
          .asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))

      val targetTable = createTargetTable(Seq("year", "month", "day"), targetSchema)
      setupInitialState(targetTable, s"$testResourceDir/lake_data_pre.psv", dataReader)

      copyResourceFileToHdfs(s"$testResourceDir/20180101_schema.json", headerPath20180101)
      copyResourceFileToHdfs(s"$testResourceDir/year=2018", sourceDirPath)
      uploadParameters(testResourceDir)

      // checking pre-conditions
      spark.read.csv(sourceDirPath.toString).count() shouldBe 3
      targetTable.read().count() shouldBe 19

      fs.exists(targetPath20180101) shouldBe true
      fs.exists(targetPath20180105) shouldBe false

      fs.exists(headerPath20180101) shouldBe true
      fs.exists(headerPath20180105) shouldBe false

      // executing load
      AppendLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      // validating result
      val expectedDataLocation =
        resolveResource(s"$testResourceDir/lake_data_post.psv", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = targetTable.read()
      actualDf.hasDiff(expectedDf) shouldBe false

      fs.exists(targetPath20180101) shouldBe true
      fs.exists(headerPath20180101) shouldBe true
    }

    Scenario("Loading data when some header files are available and schemas are different") {
      val testResourceDir = "different_schemas"
      val headerPath20180101 = new Path(headerDirPath, "year=2018/month=1/day=1/header.json")
      val headerPath20180105 = new Path(headerDirPath, "year=2018/month=1/day=5/header.json")
      val targetPath20180101 = new Path(targetDirPath, "year=2018/month=1/day=1")
      val targetPath20180105 = new Path(targetDirPath, "year=2018/month=1/day=5")

      val targetSchema =
        DataType
          .fromJson(getResourceAsText(s"$testResourceDir/target_schema.json"))
          .asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))

      val targetTable = createTargetTable(Seq("year", "month", "day"), targetSchema)
      setupInitialState(targetTable, s"$testResourceDir/lake_data_pre.psv", dataReader)
      copyResourceFileToHdfs(s"$testResourceDir/20180101_schema.json", headerPath20180101)
      prepareSourceData(
        testResourceDir,
        Seq("data_20180101-part-00000.psv", "data_20180105-part-00000.psv")
      )
      uploadParameters(testResourceDir)

      // checking pre-conditions
      spark.read.csv(sourceDirPath.toString).count() shouldBe 6
      targetTable.read().count() shouldBe 19

      fs.exists(targetPath20180101) shouldBe true
      fs.exists(targetPath20180105) shouldBe false

      fs.exists(headerPath20180101) shouldBe true
      fs.exists(headerPath20180105) shouldBe false

      val expectedSchema20180101 =
        DataType
          .fromJson(getResourceAsText(s"$testResourceDir/20180101_schema.json"))
          .asInstanceOf[StructType]
      val expectedSchema20180105 = StructType(targetSchema.dropRight(3))

      // executing load
      AppendLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      // validating result
      val expectedDataLocation =
        resolveResource(s"$testResourceDir/lake_data_post.psv", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = targetTable.read()
      actualDf.hasDiff(expectedDf) shouldBe false

      fs.exists(targetPath20180101) shouldBe true
      fs.exists(targetPath20180105) shouldBe true

      fs.exists(headerPath20180101) shouldBe true
      fs.exists(headerPath20180105) shouldBe true

      val actualSchema20180101 =
        DataType.fromJson(fs.readFile(headerPath20180101)).asInstanceOf[StructType]
      val actualSchema20180105 =
        DataType.fromJson(fs.readFile(headerPath20180105)).asInstanceOf[StructType]

      actualSchema20180101 shouldBe expectedSchema20180101
      actualSchema20180105 shouldBe expectedSchema20180105
    }

    Scenario("Loading data when some header files are available and schemas are the same") {
      val testResourceDir = "similar_schemas"
      val headerPath20180101 = new Path(headerDirPath, "year=2018/month=1/day=1/header.json")
      val headerPath20180105 = new Path(headerDirPath, "year=2018/month=1/day=5/header.json")
      val targetPath20180101 = new Path(targetDirPath, "year=2018/month=1/day=1")
      val targetPath20180105 = new Path(targetDirPath, "year=2018/month=1/day=5")

      val targetSchema =
        DataType
          .fromJson(getResourceAsText(s"$testResourceDir/target_schema.json"))
          .asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))

      val targetTable = createTargetTable(Seq("year", "month", "day"), targetSchema)
      setupInitialState(targetTable, s"$testResourceDir/lake_data_pre.psv", dataReader)
      copyResourceFileToHdfs(s"$testResourceDir/20180101_schema.json", headerPath20180101)
      prepareSourceData(
        testResourceDir,
        Seq("data_20180101-part-00000.psv", "data_20180105-part-00000.psv")
      )
      uploadParameters(testResourceDir)

      // checking pre-conditions
      spark.read.csv(sourceDirPath.toString).count() shouldBe 6
      targetTable.read().count() shouldBe 19

      fs.exists(targetPath20180101) shouldBe true
      fs.exists(targetPath20180105) shouldBe false

      fs.exists(headerPath20180101) shouldBe true
      fs.exists(headerPath20180105) shouldBe false

      val expectedSchema20180101 =
        DataType
          .fromJson(getResourceAsText(s"$testResourceDir/20180101_schema.json"))
          .asInstanceOf[StructType]
      val expectedSchema20180105 = StructType(targetSchema.dropRight(3))

      // executing load
      AppendLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      // validating result
      val expectedDataLocation =
        resolveResource(s"$testResourceDir/lake_data_post.psv", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = targetTable.read()
      actualDf.hasDiff(expectedDf) shouldBe false

      fs.exists(targetPath20180101) shouldBe true
      fs.exists(targetPath20180105) shouldBe true

      fs.exists(headerPath20180101) shouldBe true
      fs.exists(headerPath20180105) shouldBe true

      val actualSchema20180101 =
        DataType.fromJson(fs.readFile(headerPath20180101)).asInstanceOf[StructType]
      val actualSchema20180105 =
        DataType.fromJson(fs.readFile(headerPath20180105)).asInstanceOf[StructType]

      actualSchema20180101 shouldBe expectedSchema20180101
      actualSchema20180105 shouldBe expectedSchema20180105
    }

    Scenario(
      "Loading data with time partition columns when some there are duplicates for some records"
    ) {
      val testResourceDir = "duplicate_values"
      val headerPath20180101 = new Path(headerDirPath, "year=2018/month=1/day=1/header.json")
      val headerPath20180105 = new Path(headerDirPath, "year=2018/month=1/day=5/header.json")
      val targetPath20180101 = new Path(targetDirPath, "year=2018/month=1/day=1")
      val targetPath20180105 = new Path(targetDirPath, "year=2018/month=1/day=5")

      val targetSchema =
        DataType
          .fromJson(getResourceAsText(s"$testResourceDir/target_schema.json"))
          .asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))

      val targetTable = createTargetTable(Seq("year", "month", "day"), targetSchema)
      setupInitialState(targetTable, s"$testResourceDir/lake_data_pre.psv", dataReader)
      copyResourceFileToHdfs(s"$testResourceDir/20180101_schema.json", headerPath20180101)
      prepareSourceData(
        testResourceDir,
        Seq("data_20180101-part-00000.psv", "data_20180105-part-00000.psv")
      )
      uploadParameters(testResourceDir)

      // checking pre-conditions
      spark.read.csv(sourceDirPath.toString).count() shouldBe 8
      targetTable.read().count() shouldBe 19

      fs.exists(targetPath20180101) shouldBe true
      fs.exists(targetPath20180105) shouldBe false

      fs.exists(headerPath20180101) shouldBe true
      fs.exists(headerPath20180105) shouldBe false

      // executing load
      AppendLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      // validating result
      val expectedDataLocation =
        resolveResource(s"$testResourceDir/lake_data_post.psv", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = targetTable.read()
      actualDf.hasDiff(expectedDf) shouldBe false

      fs.exists(targetPath20180101) shouldBe true
      fs.exists(targetPath20180105) shouldBe true

      fs.exists(headerPath20180101) shouldBe true
      fs.exists(headerPath20180105) shouldBe true
    }

    Scenario("Loading data without partition columns from parquet files") {
      val testResourceDir = "parquet_test"
      val headerPath20180422 = new Path(headerDirPath, "year=2018/month=4/day=22/header.json")
      val targetPath20180422 = new Path(targetDirPath, "year=2018/month=4/day=22")

      val targetSchema =
        DataType
          .fromJson(getResourceAsText(s"$testResourceDir/target_schema.json"))
          .asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))

      val targetTable = createTargetTable(Seq("year", "month", "day"), targetSchema)
      prepareSourceData(testResourceDir, Seq("data_20180422-00001.parquet"))
      setupInitialState(targetTable, s"$testResourceDir/lake_data_pre.psv", dataReader)
      uploadParameters(testResourceDir)

      // checking pre-conditions
      spark.read.parquet(sourceDirPath.toString).count() shouldBe 7
      targetTable.read().count() shouldBe 19

      fs.exists(targetPath20180422) shouldBe false
      fs.exists(headerPath20180422) shouldBe false

      // executing load
      AppendLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      // validating result
      val expectedDataLocation =
        resolveResource(s"$testResourceDir/lake_data_post.psv", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = targetTable.read()
      actualDf.hasDiff(expectedDf) shouldBe false

      fs.exists(targetPath20180422) shouldBe true
      fs.exists(headerPath20180422) shouldBe true
    }
    Scenario("Loading input data that has missing columns and expecting them to be dropped") {
      val testResourceDir = "missing_columns"
      val headerPath20180422 = new Path(headerDirPath, "year=2018/month=4/day=22/header.json")
      val targetPath20180422 = new Path(targetDirPath, "year=2018/month=4/day=22")

      val targetSchema =
        DataType
          .fromJson(getResourceAsText(s"$testResourceDir/target_schema.json"))
          .asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))

      val targetTable = createTargetTable(Seq("year", "month", "day"), targetSchema)
      prepareSourceData(testResourceDir, Seq("data_20180422-00001.psv"))
      setupInitialState(targetTable, s"$testResourceDir/lake_data_pre.psv", dataReader)
      uploadParameters(testResourceDir)

      // checking pre-conditions
      spark.read.option("header", "true").csv(sourceDirPath.toString).count() shouldBe 7
      targetTable.read().count() shouldBe 19

      fs.exists(targetPath20180422) shouldBe false
      fs.exists(headerPath20180422) shouldBe false

      // executing load
      AppendLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      // validating result
      val expectedDataLocation =
        resolveResource(s"$testResourceDir/lake_data_post.psv", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = targetTable.read()
      actualDf.hasDiff(expectedDf) shouldBe false

      fs.exists(targetPath20180422) shouldBe true
      fs.exists(headerPath20180422) shouldBe true
    }

    Scenario("Loading data without partition columns from psv files") {
      val testResourceDir = "main_test"
      val headerPath20180422 = new Path(headerDirPath, "year=2018/month=4/day=22/header.json")
      val targetPath20180422 = new Path(targetDirPath, "year=2018/month=4/day=22")

      val targetSchema =
        DataType
          .fromJson(getResourceAsText(s"$testResourceDir/target_schema.json"))
          .asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema))

      val targetTable = createTargetTable(Seq("year", "month", "day"), targetSchema)
      prepareSourceData(testResourceDir, Seq("data_20180422-00001.psv"))
      setupInitialState(targetTable, s"$testResourceDir/lake_data_pre.psv", dataReader)
      uploadParameters(testResourceDir)

      // checking pre-conditions
      spark.read.option("header", "true").csv(sourceDirPath.toString).count() shouldBe 7
      targetTable.read().count() shouldBe 19

      fs.exists(targetPath20180422) shouldBe false
      fs.exists(headerPath20180422) shouldBe false

      // executing load
      AppendLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      // validating result
      val expectedDataLocation =
        resolveResource(s"$testResourceDir/lake_data_post.psv", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = targetTable.read()
      actualDf.hasDiff(expectedDf) shouldBe false

      fs.exists(targetPath20180422) shouldBe true
      fs.exists(headerPath20180422) shouldBe true
    }

    Scenario("Using Append Load Algorithm to integrate date columns as date format") {
      val testResourceDir = "partitioned_and_date_columns"
      val headerPath202020 = new Path(headerDirPath, "year=2020/week=20/header.json")
      val targetPath202020 = new Path(targetDirPath, "year=2020/week=20")

      val targetSchema =
        DataType
          .fromJson(getResourceAsText(s"$testResourceDir/target_schema.json"))
          .asInstanceOf[StructType]
      val dataReader = FileReader.newDSVFileReader(Some(targetSchema), dateFormat = "MM/dd/yyyy")

      val targetTable = createTargetTable(Seq("year", "week"), targetSchema)
      setupInitialState(targetTable, s"$testResourceDir/lake_data_pre.psv", dataReader)
      prepareSourceData(testResourceDir, Seq("new_data.psv"))
      uploadParameters(testResourceDir)

      // checking pre-conditions
      spark.read.csv(sourceDirPath.toString).count() shouldBe 4
      targetTable.read().count() shouldBe 10

      fs.exists(headerPath202020) shouldBe false
      fs.exists(targetPath202020) shouldBe false

      // executing load
      AppendLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      // validating result
      val expectedDataLocation =
        resolveResource(s"$testResourceDir/lake_data_post.psv", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = targetTable.read()

      actualDf.hasDiff(expectedDf) shouldBe false

      fs.exists(headerPath202020) shouldBe true
      fs.exists(targetPath202020) shouldBe true
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.sql(s"DROP DATABASE IF EXISTS $targetDatabase CASCADE")
    spark.sql(s"DROP DATABASE IF EXISTS $sourceDatabase CASCADE")
    spark.sql(s"CREATE DATABASE $sourceDatabase")
    spark.sql(s"CREATE DATABASE $targetDatabase")
    fs.mkdirs(sourceDirPath)
    fs.mkdirs(headerDirPath)
    fs.mkdirs(targetDirPath)
  }

  private def uploadParameters(
      testResourceDir: String,
      whichParamsFile: String = paramsFileName,
      whichParamsPath: Path = paramsFileHdfsPath
  ): Unit = copyResourceFileToHdfs(s"$testResourceDir/$whichParamsFile", whichParamsPath)

  private def createTargetTable(targetPartitions: Seq[String], targetSchema: StructType) = {
    val targetTableLocation = fs.makeQualified(new Path(hdfsRootTestPath, targetDirPath)).toString
    Table
      .newBuilder(tableName, targetDatabase, targetTableLocation, targetSchema)
      .withPartitions(targetPartitions)
      .buildParquetTable(DFSWrapper(fs.getConf), spark, external = true)
  }

  private def prepareSourceData(
      testResourceDir: String,
      sourceFiles: Seq[String],
      sourceDirPath: Path = sourceDirPath
  ): Unit =
    sourceFiles.foreach(file => copyResourceFileToHdfs(s"$testResourceDir/$file", sourceDirPath))

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
