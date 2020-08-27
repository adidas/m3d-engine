package com.adidas.analytics.feature.loads

import com.adidas.analytics.algo.loads.AppendLoad
import com.adidas.analytics.util.DFSWrapper._
import com.adidas.analytics.util.DataFormat.ParquetFormat
import com.adidas.analytics.util.OutputWriter
import com.adidas.utils.TestUtils._
import com.adidas.utils.{BaseAlgorithmTest, FileReader}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers._

class SemiStructuredLoadTest extends AnyFeatureSpec with BaseAlgorithmTest {

  private val sourceDatabase: String = "test_landing"
  private val targetDatabase: String = "test_lake"
  private val tableName: String = "test_table"

  private val paramsFileName: String = "params.json"

  private val paramsFileHdfsPath: Path = new Path(hdfsRootTestPath, paramsFileName)

  private val sourceDirPath: Path = new Path(hdfsRootTestPath, s"$sourceDatabase/$tableName/data")

  private val headerDirPath: Path = new Path(hdfsRootTestPath, s"$sourceDatabase/$tableName/header")

  private val targetDirPath: Path = new Path(hdfsRootTestPath, s"$targetDatabase/$tableName")

  Feature("Data can be loaded from source to target with append mode") {
    Scenario(
      "SemiStructured Data can be loaded with append mode by creating partitions from full path"
    ) {
      val tableNameJson: String = "test_table_semistructured"
      val paramsFileModdedRegexHdfsPath: Path = new Path(hdfsRootTestPath, paramsFileName)
      val sourceDirFullPath: Path = new Path(
        hdfsRootTestPath,
        s"$sourceDatabase/$tableNameJson/data/year=2018/month=01/day=02/"
      )
      val targetDirFullPath: Path =
        new Path(hdfsRootTestPath, s"$targetDatabase/$tableNameJson/data")
      val headerDirPathPartFromFullPath: Path =
        new Path(hdfsRootTestPath, s"$sourceDatabase/$tableNameJson/header")
      val targetDirPathPartFromFullPath: Path =
        new Path(hdfsRootTestPath, s"$targetDatabase/$tableNameJson")

      fs.mkdirs(sourceDirFullPath)
      fs.mkdirs(headerDirPathPartFromFullPath)
      fs.mkdirs(targetDirPathPartFromFullPath)
      fs.mkdirs(targetDirFullPath)

      val testResourceDir = "semistructured_json_load"
      val headerPath20180102 =
        new Path(headerDirPathPartFromFullPath, "year=2018/month=1/day=2/header.json")
      val targetPath20180102 = new Path(targetDirFullPath, "year=2018/month=1/day=2")

      val targetSchema =
        DataType
          .fromJson(getResourceAsText(s"$testResourceDir/target_schema.json"))
          .asInstanceOf[StructType]
      val targetTableLocation =
        fs.makeQualified(new Path(hdfsRootTestPath, targetDirPathPartFromFullPath)).toString

      val dataReader = FileReader.newJsonFileReader(Some(targetSchema))
      val dataFormat = ParquetFormat()
      val dataWriter = OutputWriter
        .newFileSystemWriter(targetDirFullPath.toString, dataFormat, Seq("year", "month", "day"))
      setupInitialState(s"$testResourceDir/lake_data_pre.txt", dataReader, dataWriter)
      prepareSourceData(testResourceDir, Seq("data-nodate-part-00001.txt"), sourceDirFullPath)
      uploadParameters(testResourceDir, paramsFileName, paramsFileModdedRegexHdfsPath)

      // checking pre-conditions
      spark.read.json(sourceDirFullPath.toString).count() shouldBe 6
      spark.read.parquet(targetTableLocation).count() shouldBe 5

      fs.exists(targetPath20180102) shouldBe false
      fs.exists(headerPath20180102) shouldBe false

      // executing load
      AppendLoad(spark, dfs, paramsFileModdedRegexHdfsPath.toString).run()

      // validating result
      val expectedDataLocation =
        resolveResource(s"$testResourceDir/lake_data_post.txt", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = spark.read.schema(targetSchema).parquet(targetTableLocation)
      actualDf.hasDiff(expectedDf) shouldBe false

      fs.exists(targetPath20180102) shouldBe true
      fs.exists(headerPath20180102) shouldBe true
    }

    Scenario(
      "Nested SemiStructured Data can be loaded with append mode by creating partitions from full path"
    ) {
      val tableNameJson: String = "test_table_semistructured"
      val paramsFileModdedRegexHdfsPath: Path = new Path(hdfsRootTestPath, paramsFileName)
      val sourceDirFullPath: Path = new Path(
        hdfsRootTestPath,
        s"$sourceDatabase/$tableNameJson/data/year=2018/month=01/day=02/"
      )
      val targetDirFullPath: Path =
        new Path(hdfsRootTestPath, s"$targetDatabase/$tableNameJson/data")
      val headerDirPathPartFromFullPath: Path =
        new Path(hdfsRootTestPath, s"$sourceDatabase/$tableNameJson/header")
      val targetDirPathPartFromFullPath: Path =
        new Path(hdfsRootTestPath, s"$targetDatabase/$tableNameJson")

      fs.mkdirs(sourceDirFullPath)
      fs.mkdirs(headerDirPathPartFromFullPath)
      fs.mkdirs(targetDirPathPartFromFullPath)
      fs.mkdirs(targetDirFullPath)

      val testResourceDir = "semistructured_nested_json_load"
      val headerPath20180102 =
        new Path(headerDirPathPartFromFullPath, "year=2018/month=1/day=2/header.json")
      val targetPath20180102 = new Path(targetDirFullPath, "year=2018/month=1/day=2")

      val targetSchema =
        DataType
          .fromJson(getResourceAsText(s"$testResourceDir/target_schema.json"))
          .asInstanceOf[StructType]
      val targetTableLocation =
        fs.makeQualified(new Path(hdfsRootTestPath, targetDirPathPartFromFullPath)).toString

      val dataReader = FileReader.newJsonFileReader(Some(targetSchema))
      val dataFormat = ParquetFormat()
      val dataWriter = OutputWriter
        .newFileSystemWriter(targetDirFullPath.toString, dataFormat, Seq("year", "month", "day"))
      setupInitialState(s"$testResourceDir/lake_data_pre.txt", dataReader, dataWriter)
      prepareSourceData(testResourceDir, Seq("data-nodate-part-00001.txt"), sourceDirFullPath)
      uploadParameters(testResourceDir, paramsFileName, paramsFileModdedRegexHdfsPath)

      // checking pre-conditions
      spark.read.json(sourceDirFullPath.toString).count() shouldBe 6
      spark.read.parquet(targetTableLocation).count() shouldBe 5

      fs.exists(targetPath20180102) shouldBe false
      fs.exists(headerPath20180102) shouldBe false

      // executing load
      AppendLoad(spark, dfs, paramsFileModdedRegexHdfsPath.toString).run()

      // validating result
      val expectedDataLocation =
        resolveResource(s"$testResourceDir/lake_data_post.txt", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = spark.read.schema(targetSchema).parquet(targetTableLocation)
      actualDf.hasDiff(expectedDf) shouldBe false

      fs.exists(targetPath20180102) shouldBe true
      fs.exists(headerPath20180102) shouldBe true
    }

    Scenario(
      "SemiStructured Parquet Data can be loaded with append mode by creating partitions from full path"
    ) {
      val tableNameJson: String = "test_table_semistructured"
      val paramsFileModdedRegexHdfsPath: Path = new Path(hdfsRootTestPath, paramsFileName)
      val sourceDirFullPath: Path = new Path(
        hdfsRootTestPath,
        s"$sourceDatabase/$tableNameJson/data/year=2018/month=01/day=02/"
      )
      val targetDirFullPath: Path =
        new Path(hdfsRootTestPath, s"$targetDatabase/$tableNameJson/data")
      val headerDirPathPartFromFullPath: Path =
        new Path(hdfsRootTestPath, s"$sourceDatabase/$tableNameJson/header")
      val targetDirPathPartFromFullPath: Path =
        new Path(hdfsRootTestPath, s"$targetDatabase/$tableNameJson")

      fs.mkdirs(sourceDirFullPath)
      fs.mkdirs(headerDirPathPartFromFullPath)
      fs.mkdirs(targetDirPathPartFromFullPath)
      fs.mkdirs(targetDirFullPath)

      val testResourceDir = "semistructured_parquet_test"
      val headerPath20180102 =
        new Path(headerDirPathPartFromFullPath, "year=2018/month=1/day=2/header.json")
      val targetPath20180102 = new Path(targetDirFullPath, "year=2018/month=1/day=2")

      val targetSchema =
        DataType
          .fromJson(getResourceAsText(s"$testResourceDir/target_schema.json"))
          .asInstanceOf[StructType]
      val targetTableLocation =
        fs.makeQualified(new Path(hdfsRootTestPath, targetDirPathPartFromFullPath)).toString

      val dataReader = FileReader.newJsonFileReader(Some(targetSchema))
      val dataFormat = ParquetFormat(Some(targetSchema))
      val dataWriter = OutputWriter
        .newFileSystemWriter(targetDirFullPath.toString, dataFormat, Seq("year", "month", "day"))
      setupInitialState(s"$testResourceDir/lake_data_pre.txt", dataReader, dataWriter)
      prepareSourceData(testResourceDir, Seq("sales.parquet"), sourceDirFullPath)
      uploadParameters(testResourceDir, paramsFileName, paramsFileModdedRegexHdfsPath)

      // checking pre-conditions
      spark.read.parquet(sourceDirFullPath.toString).count() shouldBe 6
      spark.read.parquet(targetTableLocation).count() shouldBe 5

      fs.exists(targetPath20180102) shouldBe false
      fs.exists(headerPath20180102) shouldBe false

      // executing load
      AppendLoad(spark, dfs, paramsFileModdedRegexHdfsPath.toString).run()

      // validating result
      val expectedDataLocation =
        resolveResource(s"$testResourceDir/lake_data_post.txt", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = spark.read.schema(targetSchema).parquet(targetTableLocation)
      actualDf.hasDiff(expectedDf) shouldBe false

      fs.exists(targetPath20180102) shouldBe true
      fs.exists(headerPath20180102) shouldBe true
    }

    Scenario("SemiStructured Data can be loaded with append mode with evolving schema") {
      val tableNameJson: String = "test_table_semistructured"
      val paramsFileModdedRegexHdfsPath: Path = new Path(hdfsRootTestPath, paramsFileName)
      val sourceDirFullPath: Path = new Path(
        hdfsRootTestPath,
        s"$sourceDatabase/$tableNameJson/data/year=2018/month=01/day=02/"
      )
      val targetDirFullPath: Path =
        new Path(hdfsRootTestPath, s"$targetDatabase/$tableNameJson/data")
      val headerDirPathPartFromFullPath: Path =
        new Path(hdfsRootTestPath, s"$sourceDatabase/$tableNameJson/header")
      val targetDirPathPartFromFullPath: Path =
        new Path(hdfsRootTestPath, s"$targetDatabase/$tableNameJson")

      fs.mkdirs(sourceDirFullPath)
      fs.mkdirs(headerDirPathPartFromFullPath)
      fs.mkdirs(targetDirPathPartFromFullPath)
      fs.mkdirs(targetDirFullPath)

      val testResourceDir = "semistructured_json_load_evolving_schema"
      val headerPath20180102 =
        new Path(headerDirPathPartFromFullPath, "year=2018/month=1/day=2/header.json")
      val targetPath20180102 = new Path(targetDirFullPath, "year=2018/month=1/day=2")

      val targetSchema =
        DataType
          .fromJson(getResourceAsText(s"$testResourceDir/target_schema.json"))
          .asInstanceOf[StructType]
      val targetTableLocation =
        fs.makeQualified(new Path(hdfsRootTestPath, targetDirPathPartFromFullPath)).toString

      val dataReader = FileReader.newJsonFileReader(Some(targetSchema))
      val dataFormat = ParquetFormat()
      val dataWriter = OutputWriter
        .newFileSystemWriter(targetDirFullPath.toString, dataFormat, Seq("year", "month", "day"))
      setupInitialState(s"$testResourceDir/lake_data_pre.txt", dataReader, dataWriter)
      prepareSourceData(testResourceDir, Seq("data-nodate-part-00001.txt"), sourceDirFullPath)
      uploadParameters(testResourceDir, paramsFileName, paramsFileModdedRegexHdfsPath)

      // checking pre-conditions
      spark.read.json(sourceDirFullPath.toString).count() shouldBe 6
      spark.read.parquet(targetTableLocation).count() shouldBe 5

      // executing load
      AppendLoad(spark, dfs, paramsFileModdedRegexHdfsPath.toString).run()

      // Executing append load with the evolved schema
      val sourceDirFullPath20180103: Path = new Path(
        hdfsRootTestPath,
        s"$sourceDatabase/$tableNameJson/data/year=2018/month=01/day=03/"
      )
      prepareSourceData(
        testResourceDir,
        Seq("data-nodate-part-00002.txt"),
        sourceDirFullPath20180103
      )
      val targetSchemaEvolved =
        DataType
          .fromJson(getResourceAsText(s"$testResourceDir/target_schema_evolved.json"))
          .asInstanceOf[StructType]
      val dataReaderEvolved = FileReader.newJsonFileReader(Some(targetSchemaEvolved))

      fs.delete(paramsFileModdedRegexHdfsPath, false)
      val paramsEvolvedFileName: String = "params_evolved.json"
      uploadParameters(testResourceDir, paramsEvolvedFileName, paramsFileModdedRegexHdfsPath)

      AppendLoad(spark, dfs, paramsFileModdedRegexHdfsPath.toString).run()
      // validating result
      val expectedDataLocation =
        resolveResource(s"$testResourceDir/lake_data_post.txt", withProtocol = true)
      val expectedDf = dataReaderEvolved.read(spark, expectedDataLocation)
      val actualDf = spark.read.schema(targetSchemaEvolved).parquet(targetTableLocation)

      actualDf.hasDiff(expectedDf) shouldBe false

      fs.exists(targetPath20180102) shouldBe true
      fs.exists(headerPath20180102) shouldBe true
    }

    Scenario("SemiStructured Data can be loaded with append mode with dropping columns") {
      val tableNameJson: String = "test_table_semistructured"
      val paramsFileModdedRegexHdfsPath: Path = new Path(hdfsRootTestPath, paramsFileName)
      val sourceDirFullPath: Path = new Path(
        hdfsRootTestPath,
        s"$sourceDatabase/$tableNameJson/data/year=2018/month=01/day=02/"
      )
      val targetDirFullPath: Path =
        new Path(hdfsRootTestPath, s"$targetDatabase/$tableNameJson/data")
      val headerDirPathPartFromFullPath: Path =
        new Path(hdfsRootTestPath, s"$sourceDatabase/$tableNameJson/header")
      val targetDirPathPartFromFullPath: Path =
        new Path(hdfsRootTestPath, s"$targetDatabase/$tableNameJson")

      fs.mkdirs(sourceDirFullPath)
      fs.mkdirs(headerDirPathPartFromFullPath)
      fs.mkdirs(targetDirPathPartFromFullPath)
      fs.mkdirs(targetDirFullPath)

      val testResourceDir = "semistructured_json_load_dropping_column"
      val headerPath20180102 =
        new Path(headerDirPathPartFromFullPath, "year=2018/month=1/day=2/header.json")
      val headerPath20180103 =
        new Path(headerDirPathPartFromFullPath, "year=2018/month=1/day=3/header.json")
      val targetPath20180102 = new Path(targetDirFullPath, "year=2018/month=1/day=2")
      val targetPath20180103 = new Path(targetDirFullPath, "year=2018/month=1/day=3")

      val targetSchema =
        DataType
          .fromJson(getResourceAsText(s"$testResourceDir/target_schema.json"))
          .asInstanceOf[StructType]
      val targetTableLocation =
        fs.makeQualified(new Path(hdfsRootTestPath, targetDirPathPartFromFullPath)).toString

      val dataReader = FileReader.newJsonFileReader(Some(targetSchema))
      val dataFormat = ParquetFormat()
      val dataWriter = OutputWriter
        .newFileSystemWriter(targetDirFullPath.toString, dataFormat, Seq("year", "month", "day"))

      setupInitialState(s"$testResourceDir/lake_data_pre.txt", dataReader, dataWriter)
      prepareSourceData(testResourceDir, Seq("data-nodate-part-00001.txt"), sourceDirFullPath)
      uploadParameters(testResourceDir, paramsFileName, paramsFileModdedRegexHdfsPath)

      // checking pre-conditions
      spark.read.json(sourceDirFullPath.toString).count() shouldBe 6
      spark.read.parquet(targetTableLocation).count() shouldBe 5

      // executing load with old schema
      AppendLoad(spark, dfs, paramsFileModdedRegexHdfsPath.toString).run()
      fs.exists(headerPath20180102) shouldBe true
      fs.exists(targetPath20180102) shouldBe true

      // clean up
      fs.delete(sourceDirFullPath, true)
      fs.delete(paramsFileModdedRegexHdfsPath, false)

      // prepare new data
      val sourceDirFullPath20180103: Path = new Path(
        hdfsRootTestPath,
        s"$sourceDatabase/$tableNameJson/data/year=2018/month=01/day=03/"
      )
      val sourceDirFullPath20180104: Path = new Path(
        hdfsRootTestPath,
        s"$sourceDatabase/$tableNameJson/data/year=2018/month=01/day=04/"
      )
      prepareSourceData(
        testResourceDir,
        Seq("data-nodate-part-00002.txt"),
        sourceDirFullPath20180103
      )
      prepareSourceData(
        testResourceDir,
        Seq("data-nodate-part-00003.txt"),
        sourceDirFullPath20180104
      )
      val targetSchemaDroppedCol =
        DataType
          .fromJson(getResourceAsText(s"$testResourceDir/target_schema_column_dropped.json"))
          .asInstanceOf[StructType]
      val dataReaderDroppedCol = FileReader.newJsonFileReader(Some(targetSchemaDroppedCol))
      val paramsDroppedColFileName: String = "params_column_dropped.json"
      uploadParameters(testResourceDir, paramsDroppedColFileName, paramsFileModdedRegexHdfsPath)

      // Executing append load with the updated schema
      AppendLoad(spark, dfs, paramsFileModdedRegexHdfsPath.toString).run()

      // validating result
      val expectedDataLocation =
        resolveResource(s"$testResourceDir/lake_data_post.txt", withProtocol = true)
      val expectedDf = dataReaderDroppedCol.read(spark, expectedDataLocation)
      val actualDf = spark.read.schema(targetSchemaDroppedCol).parquet(targetTableLocation)

      actualDf.hasDiff(expectedDf) shouldBe false

      fs.exists(headerPath20180103) shouldBe true
      fs.exists(targetPath20180103) shouldBe true

    }

    Scenario(
      "SemiStructured Data cannot be loaded when data contains more columns than target schema"
    ) {
      val tableNameJson: String = "test_table_semistructured"
      val paramsFileModdedRegexHdfsPath: Path = new Path(hdfsRootTestPath, paramsFileName)
      val sourceDirFullPath: Path = new Path(
        hdfsRootTestPath,
        s"$sourceDatabase/$tableNameJson/data/year=2018/month=01/day=02/"
      )
      val targetDirFullPath: Path =
        new Path(hdfsRootTestPath, s"$targetDatabase/$tableNameJson/data")
      val headerDirPathPartFromFullPath: Path =
        new Path(hdfsRootTestPath, s"$sourceDatabase/$tableNameJson/header")
      val targetDirPathPartFromFullPath: Path =
        new Path(hdfsRootTestPath, s"$targetDatabase/$tableNameJson")

      fs.mkdirs(sourceDirFullPath)
      fs.mkdirs(headerDirPathPartFromFullPath)
      fs.mkdirs(targetDirPathPartFromFullPath)
      fs.mkdirs(targetDirFullPath)

      val testResourceDir = "semistructured_json_load_mismatching_schema"
      val headerPath20180102 =
        new Path(headerDirPathPartFromFullPath, "year=2018/month=1/day=2/header.json")
      val targetPath20180102 = new Path(targetDirFullPath, "year=2018/month=1/day=2")

      val targetSchema =
        DataType
          .fromJson(getResourceAsText(s"$testResourceDir/target_schema.json"))
          .asInstanceOf[StructType]
      val targetTableLocation =
        fs.makeQualified(new Path(hdfsRootTestPath, targetDirPathPartFromFullPath)).toString

      val dataReader = FileReader.newJsonFileReader(Some(targetSchema))
      val dataFormat = ParquetFormat()
      val dataWriter = OutputWriter
        .newFileSystemWriter(targetDirFullPath.toString, dataFormat, Seq("year", "month", "day"))

      setupInitialState(s"$testResourceDir/lake_data_pre.txt", dataReader, dataWriter)
      prepareSourceData(testResourceDir, Seq("data-nodate-part-00001.txt"), sourceDirFullPath)
      uploadParameters(testResourceDir, paramsFileName, paramsFileModdedRegexHdfsPath)

      // checking pre-conditions
      spark.read.json(sourceDirFullPath.toString).count() shouldBe 6
      spark.read.parquet(targetTableLocation).count() shouldBe 5

      // executing load with old schema
      val caught = intercept[RuntimeException] {
        AppendLoad(spark, dfs, paramsFileModdedRegexHdfsPath.toString).run()
      }
      logger.info(caught.getMessage)
      assert(
        caught.getMessage
          .equals(s"Schema does not match the input data for some of the input folders.")
      )

      // validating result
      val expectedDataLocation =
        resolveResource(s"$testResourceDir/lake_data_post.txt", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = spark.read.schema(targetSchema).parquet(targetTableLocation)

      actualDf.hasDiff(expectedDf) shouldBe false

      fs.exists(headerPath20180102) shouldBe false
      fs.exists(targetPath20180102) shouldBe false
    }

    Scenario("SemiStructured Data cannot be loaded with wrong configuration") {
      val tableNameJson: String = "test_table_semistructured"
      val paramsFileModdedRegexHdfsPath: Path = new Path(hdfsRootTestPath, paramsFileName)
      val sourceDirFullPath: Path = new Path(
        hdfsRootTestPath,
        s"$sourceDatabase/$tableNameJson/data/year=2018/month=01/day=02/"
      )
      val targetDirFullPath: Path =
        new Path(hdfsRootTestPath, s"$targetDatabase/$tableNameJson/data")
      val headerDirPathPartFromFullPath: Path =
        new Path(hdfsRootTestPath, s"$sourceDatabase/$tableNameJson/header")
      val targetDirPathPartFromFullPath: Path =
        new Path(hdfsRootTestPath, s"$targetDatabase/$tableNameJson")

      fs.mkdirs(sourceDirFullPath)
      fs.mkdirs(headerDirPathPartFromFullPath)
      fs.mkdirs(targetDirPathPartFromFullPath)
      fs.mkdirs(targetDirFullPath)

      val testResourceDir = "semistructured_json_load_wrong_configuration"
      val targetSchema =
        DataType
          .fromJson(getResourceAsText(s"$testResourceDir/target_schema.json"))
          .asInstanceOf[StructType]

      val dataReader = FileReader.newJsonFileReader(Some(targetSchema))
      val dataFormat = ParquetFormat()
      val dataWriter = OutputWriter
        .newFileSystemWriter(targetDirFullPath.toString, dataFormat, Seq("year", "month", "day"))

      setupInitialState(s"$testResourceDir/lake_data_pre.txt", dataReader, dataWriter)
      prepareSourceData(testResourceDir, Seq("data-nodate-part-00001.txt"), sourceDirFullPath)
      uploadParameters(testResourceDir, paramsFileName, paramsFileModdedRegexHdfsPath)

      val caught = intercept[RuntimeException] {
        AppendLoad(spark, dfs, paramsFileModdedRegexHdfsPath.toString).run()
      }
      logger.info(caught.getMessage)
      assert(
        caught.getMessage.equals(
          s"Unsupported data type: unstructured in AppendLoad or the configuration file is malformed."
        )
      )
    }

    Scenario(
      "Loading semistructured data when some header files are available and schemas are the same"
    ) {
      val tableNameJson: String = "test_table_semistructured"
      val paramsFileModdedRegexHdfsPath: Path = new Path(hdfsRootTestPath, paramsFileName)
      val sourceDirFullPath20180101: Path = new Path(
        hdfsRootTestPath,
        s"$sourceDatabase/$tableNameJson/data/year=2018/month=01/day=01/"
      )
      val sourceDirFullPath20180102: Path = new Path(
        hdfsRootTestPath,
        s"$sourceDatabase/$tableNameJson/data/year=2018/month=01/day=02/"
      )
      val targetDirFullPath: Path =
        new Path(hdfsRootTestPath, s"$targetDatabase/$tableNameJson/data")
      val headerDirPathPartFromFullPath: Path =
        new Path(hdfsRootTestPath, s"$sourceDatabase/$tableNameJson/header")
      val targetDirPathPartFromFullPath: Path =
        new Path(hdfsRootTestPath, s"$targetDatabase/$tableNameJson")

      fs.mkdirs(sourceDirFullPath20180102)
      fs.mkdirs(headerDirPathPartFromFullPath)
      fs.mkdirs(targetDirPathPartFromFullPath)
      fs.mkdirs(targetDirFullPath)

      val testResourceDir = "semistructured_load_with_existing_header"
      val headerPath20180101 =
        new Path(headerDirPathPartFromFullPath, "year=2018/month=1/day=1/header.json")
      val headerPath20180102 =
        new Path(headerDirPathPartFromFullPath, "year=2018/month=1/day=2/header.json")
      val targetPath20180101 = new Path(targetDirFullPath, "year=2018/month=1/day=1")
      val targetPath20180102 = new Path(targetDirFullPath, "year=2018/month=1/day=2")

      val targetSchema =
        DataType
          .fromJson(getResourceAsText(s"$testResourceDir/target_schema.json"))
          .asInstanceOf[StructType]
      val targetTableLocation =
        fs.makeQualified(new Path(hdfsRootTestPath, targetDirPathPartFromFullPath)).toString

      val dataReader = FileReader.newJsonFileReader(Some(targetSchema))
      val dataFormat = ParquetFormat()
      val dataWriter = OutputWriter
        .newFileSystemWriter(targetDirFullPath.toString, dataFormat, Seq("year", "month", "day"))
      setupInitialState(s"$testResourceDir/lake_data_pre.txt", dataReader, dataWriter)
      copyResourceFileToHdfs(s"$testResourceDir/20180101_schema.json", headerPath20180101)
      prepareSourceData(
        testResourceDir,
        Seq("data-nodate-part-00001.txt"),
        sourceDirFullPath20180101
      )
      prepareSourceData(
        testResourceDir,
        Seq("data-nodate-part-00002.txt"),
        sourceDirFullPath20180102
      )
      uploadParameters(testResourceDir, paramsFileName, paramsFileModdedRegexHdfsPath)

      // checking pre-conditions
      spark.read.json(sourceDirFullPath20180102.toString).count() shouldBe 6
      spark.read.parquet(targetTableLocation).count() shouldBe 5

      fs.exists(targetPath20180101) shouldBe true
      fs.exists(targetPath20180102) shouldBe false

      fs.exists(headerPath20180101) shouldBe true
      fs.exists(headerPath20180102) shouldBe false

      val expectedSchema20180101 =
        DataType
          .fromJson(getResourceAsText(s"$testResourceDir/20180101_schema.json"))
          .asInstanceOf[StructType]
      val expectedSchema20180102 = StructType(targetSchema.dropRight(3))

      // executing load
      AppendLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      // validating result
      val expectedDataLocation =
        resolveResource(s"$testResourceDir/lake_data_post.txt", withProtocol = true)
      val expectedDf = dataReader.read(spark, expectedDataLocation)
      val actualDf = spark.read.schema(targetSchema).parquet(targetTableLocation)

      actualDf.hasDiff(expectedDf) shouldBe false

      fs.exists(targetPath20180101) shouldBe true
      fs.exists(targetPath20180102) shouldBe true

      fs.exists(headerPath20180101) shouldBe true
      fs.exists(headerPath20180102) shouldBe true

      val actualSchema20180101 =
        DataType.fromJson(fs.readFile(headerPath20180101)).asInstanceOf[StructType]
      val actualSchema20180105 =
        DataType.fromJson(fs.readFile(headerPath20180102)).asInstanceOf[StructType]

      actualSchema20180101 shouldBe expectedSchema20180101
      actualSchema20180105 shouldBe expectedSchema20180102
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    fs.mkdirs(sourceDirPath)
    fs.mkdirs(headerDirPath)
    fs.mkdirs(targetDirPath)
  }

  private def uploadParameters(
      testResourceDir: String,
      whichParamsFile: String = paramsFileName,
      whichParamsPath: Path = paramsFileHdfsPath
  ): Unit = copyResourceFileToHdfs(s"$testResourceDir/$whichParamsFile", whichParamsPath)

  private def prepareSourceData(
      testResourceDir: String,
      sourceFiles: Seq[String],
      sourceDirPath: Path = sourceDirPath
  ): Unit =
    sourceFiles.foreach(file => copyResourceFileToHdfs(s"$testResourceDir/$file", sourceDirPath))

  private def setupInitialState(
      localDataFile: String,
      dataReader: FileReader,
      dataWriter: OutputWriter
  ): Unit = {
    val initialDataLocation = resolveResource(localDataFile, withProtocol = true)
    dataWriter.write(dfs, dataReader.read(spark, initialDataLocation))
  }
}
