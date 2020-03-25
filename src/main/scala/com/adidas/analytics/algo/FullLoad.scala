package com.adidas.analytics.algo

import com.adidas.analytics.config.FullLoadConfiguration
import com.adidas.analytics.algo.FullLoad._
import com.adidas.analytics.algo.core.{Algorithm, TableStatistics}
import com.adidas.analytics.algo.core.Algorithm.WriteOperation
import com.adidas.analytics.algo.shared.DateComponentDerivation
import com.adidas.analytics.util.DFSWrapper._
import com.adidas.analytics.util.DataFormat.{DSVFormat, ParquetFormat}
import com.adidas.analytics.util._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}


final class FullLoad protected(val spark: SparkSession, val dfs: DFSWrapper, val configLocation: String)
  extends Algorithm with WriteOperation with FullLoadConfiguration with DateComponentDerivation  with TableStatistics {

  val currentHdfsDir: String = HiveTableAttributeReader(targetTable, spark).getTableLocation

  override protected def read(): Vector[DataFrame] = {
    createBackupTable()

    val dataFormat: DataFormat = fileFormat match {
      case "parquet" => ParquetFormat(Some(targetSchema))
      case "dsv" => DSVFormat(Some(targetSchema))
      case _ => throw new RuntimeException(s"Unsupported input data format $fileFormat.")
    }

    readInputData(dataFormat)
  }

  override protected def transform(dataFrames: Vector[DataFrame]): Vector[DataFrame] = {
    withDatePartitions(dataFrames)
  }

  override protected def write(dataFrames: Vector[DataFrame]): Vector[DataFrame] = {
    Try{
      super.write(dataFrames)
    } match {
      case Failure(exception) =>
        logger.error(s"Handled Exception: ${exception.getMessage}. " +
          s"Start Rolling Back the Full Load of table: ${targetTable}!")
        recoverFailedWrite()
        cleanupDirectory(backupDir)
        throw new RuntimeException(exception.getMessage)
      case Success(outputDaframe) =>
        restoreTable()
        outputDaframe
    }

  }

  override protected def updateStatistics(dataFrames: Vector[DataFrame]): Unit = {
      if (computeTableStatistics && dataType == STRUCTURED) {
        if(targetPartitions.nonEmpty) {
          dataFrames.foreach(df => computeStatisticsForTablePartitions(df,targetTable, targetPartitions))
        }
        computeStatisticsForTable(Option(targetTable))
      }

  }

  private def createBackupTable(): Unit = {
    createDirectory(backupDir)

    // backup the data from the current dir because currently data directory for full load is varying

    backupDataDirectory(currentHdfsDir, backupDir)

    try {
      dropAndRecreateTableInNewLocation(targetTable, backupDir, targetPartitions)
    } catch {
      case e: Throwable =>
        logger.error("Data backup failed", e)
        logger.info(s"Restoring previous state $backupDir -> $currentDir")
        recoverFailBackup()
        cleanupDirectory(backupDir)
        throw new RuntimeException("Unable to change table location.", e)
    }
  }

  private def readInputData(dataFormat: DataFormat): Vector[DataFrame] ={
    try {
      Vector(dataFormat.read(spark.read.options(sparkReaderOptions), inputDir))
    } catch {
      case e: Throwable =>
        logger.error("Data reading failed", e)
        recoverFailedRead()
        cleanupDirectory(backupDir)
        throw new RuntimeException("Unable to read input location.", e)
    }
  }

  private def createDirectory(dir: String): Unit = {
    val path = new Path(dir)

    logger.info(s"Creating directory ${path.toString}")
    val fs = dfs.getFileSystem(path)
    fs.createDirIfNotExists(path)
  }

  private def cleanupDirectory(dir: String): Unit = {
    DistCpLoadHelper.cleanupDirectoryContent(dfs, dir)
  }

  private def backupDataDirectory(sourceDir: String, destinationDir: String): Unit = {
    DistCpLoadHelper.cleanupDirectoryContent(dfs, destinationDir)
    DistCpLoadHelper.backupDirectoryContent(dfs, sourceDir, destinationDir)
  }

  private def dropAndRecreateTableInNewLocation(table: String, destinationDir: String, targetPartitions: Seq[String]): Unit = {
    val tempTable: String = s"${table}_temp"
    val tempTableDummyLocation: String = s"/tmp/$table"

    //create a temp table like the target table in a dummy location to preserve the schema
    createTable(table, tempTable, tempTableDummyLocation)

    //create the target table like the temp table with data in the new directory
    createTable(tempTable, table, destinationDir)

    if (targetPartitions.nonEmpty) {
      spark.catalog.recoverPartitions(table)
    }
  }

  private def createTable(sourceTable: String, destinationTable: String, location: String): Unit ={
    val createTempTableWithLocation = createExternalTableStatement(sourceTable, destinationTable, location)
    spark.sql(createTempTableWithLocation)
    spark.sql(s"DROP TABLE IF EXISTS $sourceTable")
  }

  private def withDatePartitions(dataFrames: Vector[DataFrame]): Vector[DataFrame] ={
    logger.info("Adding partitioning information if needed")
    try {
      if (targetPartitions.nonEmpty) {
        dataFrames.map(df => df.transform(withDateComponents(partitionSourceColumn, partitionSourceColumnFormat, targetPartitions)))
      } else {
        dataFrames
      }
    } catch {
      case e: Throwable =>
        logger.error("Cannot add partitioning information for data frames.", e)
        logger.info(s"Restoring previous state $backupDir -> $currentDir")
        recoverFailedWrite()
        cleanupDirectory(backupDir)
        throw new RuntimeException("Unable to transform data frames.", e)
    }
  }

  private def restoreTable(): Unit ={
    try {
      dropAndRecreateTableInNewLocation(targetTable, currentDir, targetPartitions)
    } catch {
      case e: Throwable =>
        logger.error("Data writing failed", e)
        logger.info(s"Restoring previous state $backupDir -> $currentDir")
        recoverFailedWrite()
        throw new RuntimeException("Unable to change table location ", e)
    } finally {
      cleanupDirectory(backupDir)
    }
  }

  private def recoverFailBackup(): Unit = {
    val tempTable: String = s"${targetTable}_temp"

    try {
      createTable(tempTable, targetTable, currentDir)
    } catch {
      case e: Exception => logger.warn(s"Failure when restoring table from temp table",e)
    } finally {
      spark.sql(s"DROP TABLE IF EXISTS $tempTable")
    }

    if (targetPartitions.nonEmpty) {
      spark.catalog.recoverPartitions(targetTable)
    }
  }

  private def recoverFailedRead(): Unit = {
    dropAndRecreateTableInNewLocation(targetTable, currentDir, targetPartitions)
  }

  private def recoverFailedWrite(): Unit = {
    restoreDirectoryContent(currentDir, backupDir)
    dropAndRecreateTableInNewLocation(targetTable, currentDir, targetPartitions)
  }

  private def restoreDirectoryContent(sourceDir: String, backupDir: String): Unit = {
    DistCpLoadHelper.restoreDirectoryContent(dfs, sourceDir, backupDir)
  }

}


object FullLoad {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def apply(spark: SparkSession, dfs: DFSWrapper, configLocation: String): FullLoad = {
    new FullLoad(spark, dfs, configLocation)
  }

  private def createExternalTableStatement(sourceTable: String, destTable:String, location: String) : String  = {
    s"CREATE TABLE $destTable LIKE $sourceTable LOCATION '$location'"
  }
}