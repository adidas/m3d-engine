package com.adidas.analytics.feature

import com.adidas.analytics.algo.DeltaLoad
import com.adidas.analytics.util.{DFSWrapper, LoadMode}
import com.adidas.utils.TestUtils._
import com.adidas.utils.{BaseAlgorithmTest, FileReader, Table}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest.FeatureSpec
import org.scalatest.Matchers._

class DeltaLoadTest extends FeatureSpec with BaseAlgorithmTest {

  private val testDatabase: String = "test_lake"
  private val dataTableName: String = "delta_load_active_data"
  private val deltaTableName: String = "delta_load_delta_data"
  private val controlTableName: String = "control_table"

  private val dsvReader: FileReader = FileReader.newDSVFileReader(header = true)

  private var dataTable: Table = _
  private var deltaTable: Table = _
  private var controlTable: Table = _

  private var paramsFileHdfsPath: Path = _


  feature("Correct execution of delta load with delta records from csv file") {
    scenario("Correct execution of delta load partitioned by date/time columns") {
      setupEnvironment(Seq("year", "month", "day"), "params.json", "csv_test")
      DeltaLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      val actualDf = dataTable.read()
      val expectedDf = controlTable.read()
      actualDf.hasDiff(expectedDf) shouldBe false
    }

    scenario("Correct execution of delta load partitioned by customer") {
      setupEnvironment(Seq("customer"), "params_part.json", "csv_test")

      DeltaLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      val actualDf = dataTable.read()
      val expectedDf = controlTable.read()
      actualDf.hasDiff(expectedDf) shouldBe false
    }
  }

  feature("Correct execution of delta load with delta records from parquet file") {
    scenario("Delta Init") {
      val testResourceDir = "parquet_test_delta_init"
//      setupEnvironment(Seq("year", "month", "day"), "params.json", testResourceDir)
//      createParquetFileFromDSVfileandWriteToHDSF(testResourceDir, "delta_data.psv", "delta_schema.json")

      dataTable = createTableWithSchema(dataTableName, Seq("year", "month", "day"), testResourceDir, None, "active_data_schema.json")
      controlTable = createTableWithSchema(controlTableName, Seq("year", "month", "day"), testResourceDir, Some("active_data_post.psv"), "active_data_schema.json")

      createParquetFileFromDSVfileandWriteToHDSF(testResourceDir, "delta_data.psv", "delta_data_schema.json")

      placeParametersFile(testResourceDir, "params.json")

      DeltaLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      val actualDf = dataTable.read()
      val expectedDf = controlTable.read()
      actualDf.hasDiff(expectedDf) shouldBe false
    }

    scenario("Delta Merge Partitioned") {
      val testResourceDir = "parquet_test_delta_merge_partitioned"
//      setupEnvironment(Seq("year", "month", "day"), "params.json", testResourceDir)
//      createParquetFileFromDSVfileandWriteToHDSF(testResourceDir, "delta_data.psv", "delta_schema.json")

      dataTable= createTableWithSchema(dataTableName, Seq("year", "month", "day"), testResourceDir, Some("active_data_pre.psv"), "active_data_schema.json")
      controlTable = createTableWithSchema(controlTableName, Seq("year", "month", "day"), testResourceDir, Some("active_data_post.psv"), "active_data_schema.json")

      createParquetFileFromDSVfileandWriteToHDSF(testResourceDir, "delta_data.psv", "delta_data_schema.json")

      placeParametersFile(testResourceDir, "params.json")

      DeltaLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      val actualDf = dataTable.read()
      val expectedDf = controlTable.read()
      actualDf.hasDiff(expectedDf) shouldBe false
    }

  scenario("Delta Merge Unpartitioned") {
    val testResourceDir = "parquet_test_delta_merge_unpartitioned"
    //      setupEnvironment(Seq("year", "month", "day"), "params.json", testResourceDir)
    //      createParquetFileFromDSVfileandWriteToHDSF(testResourceDir, "delta_data.psv", "delta_schema.json")

    dataTable= createTableWithSchema(dataTableName, Seq("year", "month", "day"), testResourceDir, Some("active_data_pre.psv"), "active_data_schema.json")
    controlTable = createTableWithSchema(controlTableName, Seq("year", "month", "day"), testResourceDir, Some("active_data_post.psv"), "active_data_schema.json")


    createParquetFileFromDSVfileandWriteToHDSF(testResourceDir, "delta_data.psv", "delta_data_schema.json")

    placeParametersFile(testResourceDir, "params.json")

    DeltaLoad(spark, dfs, paramsFileHdfsPath.toString).run()

    val actualDf = dataTable.read()
    val expectedDf = controlTable.read()
    actualDf.hasDiff(expectedDf) shouldBe false
  }

    scenario("Delta Merge with additional columns in the delta file") {
      val testResourceDir = "parquet_test_delta_merge_additional_columns"
      //      setupEnvironment(Seq("year", "month", "day"), "params.json", testResourceDir)
      //      createParquetFileFromDSVfileandWriteToHDSF(testResourceDir, "delta_data.psv", "delta_schema.json")

      dataTable= createTableWithSchema(dataTableName, Seq("year", "month", "day"), testResourceDir, Some("active_data_pre.psv"), "active_data_schema.json")
      controlTable = createTableWithSchema(controlTableName, Seq("year", "month", "day"), testResourceDir, Some("active_data_post.psv"), "active_data_schema.json")


      createParquetFileFromDSVfileandWriteToHDSF(testResourceDir, "delta_data.psv", "delta_data_schema.json")

      placeParametersFile(testResourceDir, "params.json")

      DeltaLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      val actualDf = dataTable.read()
      val expectedDf = controlTable.read()
      actualDf.hasDiff(expectedDf) shouldBe false
    }

    scenario("Delta Merge with missing columns in the delta file") {
      val testResourceDir = "parquet_test_delta_merge_missing_columns"
      //      setupEnvironment(Seq("year", "month", "day"), "params.json", testResourceDir)
      //      createParquetFileFromDSVfileandWriteToHDSF(testResourceDir, "delta_data.psv", "delta_schema.json")

      dataTable= createTableWithSchema(dataTableName, Seq("year", "month", "day"), testResourceDir, Some("active_data_pre.psv"), "active_data_schema.json")
      controlTable = createTableWithSchema(controlTableName, Seq("year", "month", "day"), testResourceDir, Some("active_data_post.psv"), "active_data_schema.json")


      createParquetFileFromDSVfileandWriteToHDSF(testResourceDir, "delta_data.psv", "delta_data_schema.json")

      placeParametersFile(testResourceDir, "params.json")

      DeltaLoad(spark, dfs, paramsFileHdfsPath.toString).run()

      val actualDf = dataTable.read()
      val expectedDf = controlTable.read()
      actualDf.hasDiff(expectedDf) shouldBe false
    }
}

  private def createParquetFileFromDSVfileandWriteToHDSF(testResourceDir: String, dsvResource: String, schemaResource: String): Unit = {
    val dsvFilePath = resolveResource(s"$testResourceDir/$dsvResource", withProtocol = true)
    val deltaDataSchema = DataType.fromJson(getResourceAsText(s"$testResourceDir/$schemaResource")).asInstanceOf[StructType]
    spark.read
      .option("header", "true")
      .option("delimiter", "|")
      .schema(deltaDataSchema)
      .csv(dsvFilePath)
      .write.parquet(s"hdfs:/tmp/tests/${dsvResource.replace(".psv", ".parquet")}")
  }

  private def setupEnvironment(targetPartitions: Seq[String], paramsFileName: String, testResourceDir: String): Unit = {
    def createTable(tableName: String, dataFile: String, parquet: Boolean): Table = {
      val dataLocation = resolveResource(dataFile, withProtocol = true)
      val schema: StructType = dsvReader.read(spark, dataLocation).schema
      val tableLocation = fs.makeQualified(new Path(hdfsRootTestPath, s"$testDatabase/$tableName"))

      val tableBuilder = Table.newBuilder(tableName, testDatabase, tableLocation.toString, schema)
        .withPartitions(targetPartitions)

      val table = if (parquet) {
        tableBuilder.buildParquetTable(DFSWrapper(fs.getConf), spark, external = true)
      } else {
        tableBuilder.buildDSVTable(DFSWrapper(fs.getConf), spark, external = true)
      }

      table.write(Seq(dataLocation), dsvReader, LoadMode.OverwritePartitionsWithAddedColumns, fillNulls = true)
      table
    }

    dataTable = createTable(dataTableName, s"$testResourceDir/active_data_pre.psv", parquet = true)
    if (testResourceDir == "csv_test") {
      deltaTable = createTable(deltaTableName, s"$testResourceDir/delta_data.psv", parquet = false)
    }
    controlTable = createTable(controlTableName, s"$testResourceDir/active_data_post.psv", parquet = true)

    paramsFileHdfsPath = new Path(hdfsRootTestPath, paramsFileName)
    copyResourceFileToHdfs(s"$testResourceDir/$paramsFileName", paramsFileHdfsPath)
  }

  private def createTableWithSchema(tableName: String, targetPartitions: Seq[String], testResourceDir: String, dsvFileName: Option[String], schemaFileName: String): Table = {
    val tableLocation = fs.makeQualified(new Path(hdfsRootTestPath, s"$testDatabase/$tableName"))
    val schema = DataType.fromJson(getResourceAsText(s"$testResourceDir/$schemaFileName")).asInstanceOf[StructType]


    val table = Table.newBuilder(tableName, testDatabase, tableLocation.toString, schema)
      .withPartitions(targetPartitions)
      .buildParquetTable(DFSWrapper(fs.getConf), spark, external = true)

    dsvFileName match {
      case Some(fileName) =>
        val dataLocation = resolveResource(s"$testResourceDir/$fileName", withProtocol = true)
        table.write(Seq(dataLocation), dsvReader, LoadMode.OverwritePartitionsWithAddedColumns, fillNulls = true)
        table
      case None => table
    }
  }

  def placeParametersFile(testResourceDir: String, paramsFileName: String): Unit ={
    paramsFileHdfsPath = new Path(hdfsRootTestPath, paramsFileName)
    copyResourceFileToHdfs(s"$testResourceDir/$paramsFileName", paramsFileHdfsPath)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.sql(s"DROP DATABASE IF EXISTS $testDatabase CASCADE")
    spark.sql(s"CREATE DATABASE $testDatabase")
  }
}
