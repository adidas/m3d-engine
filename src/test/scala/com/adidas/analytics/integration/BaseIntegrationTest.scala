package com.adidas.analytics.integration

import com.adidas.analytics.util.{DFSWrapper, LoadMode}
import com.adidas.utils.{BaseAlgorithmTest, FileReader, Table}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.StructType

trait BaseIntegrationTest extends BaseAlgorithmTest {

  protected val sourceDatabase: String = "test_landing"
  protected val targetDatabase: String = "test_lake"
  protected val tableName: String = "test_table"

  protected val paramsFileName: String = "params.json"

  protected val paramsFileHdfsPath: Path = new Path(hdfsRootTestPath, paramsFileName)

  protected val sourceDirPath: Path = new Path(hdfsRootTestPath, s"$sourceDatabase/$tableName/data")

  protected val headerDirPath: Path =
    new Path(hdfsRootTestPath, s"$sourceDatabase/$tableName/header")

  protected val targetDirPath: Path = new Path(hdfsRootTestPath, s"$targetDatabase/$tableName")

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

  protected def uploadParameters(
      testResourceDir: String,
      whichParamsFile: String = paramsFileName,
      whichParamsPath: Path = paramsFileHdfsPath
  ): Unit = copyResourceFileToHdfs(s"$testResourceDir/$whichParamsFile", whichParamsPath)

  protected def createTargetTable(
      targetPartitions: Seq[String],
      targetSchema: StructType
  ): Table = {
    val targetTableLocation = fs.makeQualified(new Path(hdfsRootTestPath, targetDirPath)).toString
    Table
      .newBuilder(tableName, targetDatabase, targetTableLocation, targetSchema)
      .withPartitions(targetPartitions)
      .buildParquetTable(DFSWrapper(fs.getConf), spark, external = true)
  }

  protected def prepareSourceData(
      testResourceDir: String,
      sourceFiles: Seq[String],
      sourceDirPath: Path = sourceDirPath
  ): Unit =
    sourceFiles.foreach(file => copyResourceFileToHdfs(s"$testResourceDir/$file", sourceDirPath))

  protected def prepareSourceData(sourceFiles: Seq[String]): Unit =
    sourceFiles.foreach { file =>
      logger.info(s"copyResourceFileToHdfs $file to ${sourceDirPath.toString}")
      copyResourceFileToHdfs(s"$file", sourceDirPath)
    }

  protected def setupInitialState(
      targetTable: Table,
      localDataFile: String,
      dataReader: FileReader
  ): Unit = {
    val initialDataLocation = resolveResource(localDataFile, withProtocol = true)
    targetTable
      .write(Seq(initialDataLocation), dataReader, LoadMode.OverwritePartitionsWithAddedColumns)
  }

  protected def createPartitionedTargetTable(
      targetPartitions: Seq[String],
      targetSchema: StructType,
      tableName: String
  ): Table = {
    val targetTableLocation = fs.makeQualified(new Path(hdfsRootTestPath, targetDirPath)).toString
    Table
      .newBuilder(tableName, targetDatabase, targetTableLocation, targetSchema)
      .withPartitions(targetPartitions)
      .buildParquetTable(DFSWrapper(fs.getConf), spark, external = true)
  }

  protected def prepareDefaultSourceData(sourceDataFile: String): Unit =
    prepareSourceData(Seq(sourceDataFile))

}
