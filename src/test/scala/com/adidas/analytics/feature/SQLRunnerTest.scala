package com.adidas.analytics.feature

import com.adidas.utils.TestUtils._
import com.adidas.analytics.algo.SQLRunner
import com.adidas.analytics.util.{DFSWrapper, CatalogTableManager, LoadMode}
import com.adidas.utils.{BaseAlgorithmTest, FileReader, Table}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.StructType
import org.scalatest.featurespec.AnyFeatureSpec
import org.scalatest.matchers.should.Matchers._

class SQLRunnerTest extends AnyFeatureSpec with BaseAlgorithmTest {

  private val sourceDatabase: String = "test_landing"
  private val targetDatabase: String = "test_lake"
  private val tableName: String = "bi_sales_order"

  private val paramsFileName: String = "params.json"

  private val paramsFileHdfsPath: Path = new Path(hdfsRootTestPath, paramsFileName)

  Feature("Data can be loaded with Hive runner") {
    Scenario("Data can be loaded from source to target with full mode") {
      val dsvReader: FileReader = FileReader.newDSVFileReader(header = true)
      val sourceDataLocation = resolveResource("sql_runner_dataset.psv", withProtocol = true)

      val schema: StructType = dsvReader.read(spark, sourceDataLocation).schema

      val sourceTableLocation =
        fs.makeQualified(new Path(hdfsRootTestPath, s"$sourceDatabase/$tableName"))
      val oldTargetTableLocation = fs.makeQualified(
        new Path(hdfsRootTestPath, s"$targetDatabase/$tableName/20180505_020927_EDT")
      )
      val newTargetTableLocation = fs.makeQualified(
        new Path(hdfsRootTestPath, s"$targetDatabase/$tableName/20190201_020927_EDT")
      )
      copyResourceFileToHdfs(paramsFileName, paramsFileHdfsPath)

      val sourceTable =
        Table
          .newBuilder(tableName, sourceDatabase, sourceTableLocation.toString, schema)
          .buildDSVTable(DFSWrapper(fs.getConf), spark, external = true)

      val targetTable =
        Table
          .newBuilder(tableName, targetDatabase, oldTargetTableLocation.toString, schema)
          .withPartitions(Seq("year", "month", "day"))
          .buildDSVTable(DFSWrapper(fs.getConf), spark, external = true)

      sourceTable.write(Seq(sourceDataLocation), dsvReader, LoadMode.OverwritePartitions)

      sourceTable.read().count() shouldBe 19

      SQLRunner(spark, paramsFileHdfsPath.toString).run()

      targetTable.read().count() shouldBe 19

      val fullTargetTableName = s"$targetDatabase.$tableName"
      spark.catalog.tableExists(s"${fullTargetTableName}_swap") shouldBe false
      sourceTable.read().hasDiff(targetTable.read()) shouldBe false

      val actualTableLocation =
        new Path(CatalogTableManager(fullTargetTableName, spark).getTableLocation)
      actualTableLocation shouldBe newTargetTableLocation
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.sql(s"DROP DATABASE IF EXISTS $targetDatabase CASCADE")
    spark.sql(s"DROP DATABASE IF EXISTS $sourceDatabase CASCADE")
    spark.sql(s"CREATE DATABASE $sourceDatabase")
    spark.sql(s"CREATE DATABASE $targetDatabase")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set("hive.exec.dynamic.partition", "true")
    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
  }
}
