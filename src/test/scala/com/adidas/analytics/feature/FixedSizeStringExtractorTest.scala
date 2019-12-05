package com.adidas.analytics.feature

import com.adidas.utils.TestUtils._
import com.adidas.analytics.algo.FixedSizeStringExtractor
import com.adidas.analytics.util.{DFSWrapper, LoadMode}
import com.adidas.utils.{BaseAlgorithmTest, FileReader, Table}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{DataType, StructType}
import org.scalatest.FeatureSpec
import org.scalatest.Matchers._


class FixedSizeStringExtractorTest extends FeatureSpec with BaseAlgorithmTest {

  private val paramsFileName: String = "params.json"
  private val paramsFileHdfsPath: Path = new Path(hdfsRootTestPath, paramsFileName)

  private val database: String = "test_lake"
  private val sourceTableName: String = "source_table"
  private val targetTableName: String = "target_table"

  private var sourceTable: Table = _
  private var targetTable: Table = _


  feature("Fixed-size string can be extractor from the input table and stored to the output table") {
    scenario("Extracted strings match to the target schema") {
      val testResourceDir = "matched_schema"
      prepare(testResourceDir)

      // check pre-conditions
      sourceTable.read().count() shouldBe 14
      targetTable.read().count() shouldBe 0

      // execute the logic
      FixedSizeStringExtractor(spark, dfs, paramsFileHdfsPath.toString).run()

      // read  expected data
      val testDataReader = FileReader.newDSVFileReader(Some(targetTable.schema))
      val expectedDataLocation = resolveResource(s"$testResourceDir/lake_data_post.psv", withProtocol = true)
      val expectedDf = testDataReader.read(spark, expectedDataLocation)

      // compare the result
      val actualDf = targetTable.read()
      actualDf.hasDiff(expectedDf) shouldBe false
    }

    scenario("Number of string to extract is less than the number of non-partition fields in the target schema") {
      val testResourceDir = "non_matched_schema1"
      prepare(testResourceDir, initialData = false)

      // execute the logic
      val caught = intercept[RuntimeException] {
        FixedSizeStringExtractor(spark, dfs, paramsFileHdfsPath.toString).run()
      }

      caught.getMessage shouldBe "Field positions do not correspond to the target schema"
    }

    scenario("Number of string to extract is greater than the number of non-partition fields in the target schema") {
      val testResourceDir = "non_matched_schema2"
      prepare(testResourceDir, initialData = false)

      // execute the logic
      val caught = intercept[RuntimeException] {
        FixedSizeStringExtractor(spark, dfs, paramsFileHdfsPath.toString).run()
      }

      caught.getMessage shouldBe "Field positions do not correspond to the target schema"
    }

    scenario("Data matches to the schema and partitioning type is year/month") {
      val testResourceDir = "matched_schema_partitioned"
      prepare(testResourceDir, Seq("year", "month"))

      // check pre-conditions
      sourceTable.read().count() shouldBe 14
      targetTable.read().count() shouldBe 0

      // execute the logic
      FixedSizeStringExtractor(spark, dfs, paramsFileHdfsPath.toString).run()

      // read  expected data
      val testDataReader = FileReader.newDSVFileReader(Some(targetTable.schema))
      val expectedDataLocation = resolveResource(s"$testResourceDir/lake_data_post.psv", withProtocol = true)
      val expectedDf = testDataReader.read(spark, expectedDataLocation)

      // compare the result
      val actualDf = targetTable.read()
      actualDf.hasDiff(expectedDf) shouldBe false
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.sql(s"DROP DATABASE IF EXISTS $database CASCADE")
    spark.sql(s"CREATE DATABASE $database")
  }

  private def createTable(tableName: String, database: String, schema: StructType, targetPartitions: Seq[String]): Table = {
    val table = Table.newBuilder(tableName, database, fs.makeQualified(new Path(hdfsRootTestPath, tableName)).toString, schema)

    if (targetPartitions.nonEmpty) {
      table.withPartitions(targetPartitions).buildParquetTable(DFSWrapper(fs.getConf), spark, external = true)
    } else {
      table.buildParquetTable(DFSWrapper(fs.getConf), spark, external = true)
    }
  }

  private def prepare(testResourceDir: String, targetPartitions: Seq[String] = Seq.empty, initialData: Boolean = true): Unit = {
    val sourceSchema = DataType.fromJson(getResourceAsText(s"$testResourceDir/source_schema.json")).asInstanceOf[StructType]
    val targetSchema = DataType.fromJson(getResourceAsText(s"$testResourceDir/target_schema.json")).asInstanceOf[StructType]

    // copy job parameters to HDFS
    copyResourceFileToHdfs(s"$testResourceDir/$paramsFileName", paramsFileHdfsPath)

    // create tables
    sourceTable = createTable(sourceTableName, database, sourceSchema, targetPartitions)
    targetTable = createTable(targetTableName, database, targetSchema, targetPartitions)

    // add data to source table
    if (initialData) {
      val testDataReader = FileReader.newDSVFileReader(Some(sourceSchema))
      val sourceDataFile = resolveResource(s"$testResourceDir/source_data.psv", withProtocol = true)
      val sourceDf = testDataReader.read(spark, sourceDataFile)
      sourceTable.write(sourceDf, LoadMode.OverwriteTable)
    }
  }
}
