package com.adidas.analytics.util

import com.adidas.analytics.algo.core.Metadata
import com.adidas.analytics.util.DataFrameUtils._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrameWriter, _}
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

/**
  * Base trait for classes which are capable of persisting DataFrames
  */
sealed abstract class OutputWriter {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  def targetPartitions: Seq[String]

  def options: Map[String, String]

  def write(dfs: DFSWrapper, df: DataFrame): DataFrame

  protected def getWriter(df: DataFrame): DataFrameWriter[Row] = {
    if (targetPartitions.nonEmpty) {
      df.write.partitionBy(targetPartitions: _*)
    } else {
      df.write
    }
  }
}


object OutputWriter {

  /**
    * Factory method which creates TableWriter
    *
    * @param table target table
    * @param targetPartitions specifies how data should be partitioned
    * @param options options which are provided to Spark DataFrameWriter
    * @param loadMode specifies LoadMode for the writer (see more details for SaveMode in Spark documentation)
    * @return TableWriter
    */
  def newTableWriter(table: String, targetPartitions: Seq[String] = Seq.empty, options: Map[String, String] = Map.empty,
                     loadMode: LoadMode = LoadMode.OverwritePartitionsWithAddedColumns): TableWriter = {
    TableWriter(table, targetPartitions, options, loadMode)
  }

  /**
    * Factory method which creates TableLocationWriter
    *
    * @param table target table which location is used for writing data to
    * @param format format of result data
    * @param targetPartitions specifies how data should be partitioned
    * @param options options which are provided to Spark DataFrameWriter
    * @param loadMode specifies LoadMode for the writer (see more details for SaveMode in Spark documentation)
    * @return TableLocationWriter
    */
  def newTableLocationWriter(table: String, format: DataFormat, targetPartitions: Seq[String] = Seq.empty,
                             options: Map[String, String] = Map.empty, loadMode: LoadMode = LoadMode.OverwritePartitionsWithAddedColumns,
                             metadataConfiguration: Metadata): TableLocationWriter = {
    TableLocationWriter(table, format, targetPartitions, options, loadMode, metadataConfiguration)
  }

  /**
    * Factory method which creates FileSystemWriter
    *
    * @param location output location on the filesystem
    * @param format format of result data
    * @param targetPartitions specifies how data should be partitioned
    * @param options options which are provided to Spark DataFrameWriter
    * @param loadMode specifies LoadMode for the writer (see more details for SaveMode in Spark documentation)
    * @return FileSystemWriter
    */
  def newFileSystemWriter(location: String, format: DataFormat, targetPartitions: Seq[String] = Seq.empty,
                          options: Map[String, String] = Map.empty, loadMode: LoadMode = LoadMode.OverwritePartitionsWithAddedColumns): FileSystemWriter = {
    FileSystemWriter(location, format, targetPartitions, options, loadMode)
  }

  /**
    * Base trait for writers which are capable of writing data in safer way.
    * The data is written in three steps:
    *  - write data to a temporary location
    *  - create backups for existing partitions which are supposed to be replaced
    *  - move new partitions to the final location
    */
  sealed trait AtomicWriter extends OutputWriter {

    def format: DataFormat

    def writeWithBackup(dfs: DFSWrapper, df: DataFrame): DataFrame

    protected def writeUnsafe(dfs: DFSWrapper, df: DataFrame, finalLocation: String, loadMode: LoadMode): DataFrame = {
      val finalPath = new Path(finalLocation)
      val fs = dfs.getFileSystem(finalPath)
      if (loadMode == LoadMode.OverwriteTable) {
        HadoopLoadHelper.cleanupDirectoryContent(fs, finalPath)
      }
      write(fs, df, finalPath, loadMode)
    }

    protected def writeSafe(dfs: DFSWrapper, df: DataFrame, finalLocation: String, loadMode: LoadMode): DataFrame = {
      Try {
        lazy val partitionsCriteria = df.collectPartitions(targetPartitions)

        val finalPath = new Path(finalLocation)
        val fs = dfs.getFileSystem(finalPath)

        val tempPath = HadoopLoadHelper.buildTempPath(finalPath)
        val tempDataPath = new Path(tempPath, "data")
        val tempBackupPath = new Path(tempPath, "backup")

        fs.delete(tempPath, true)

        loadMode match {
          case LoadMode.OverwriteTable =>
            loadTable(fs, df, finalPath, tempDataPath, tempBackupPath)
          case LoadMode.OverwritePartitions =>
            loadPartitions(fs, df, finalPath, tempDataPath, tempBackupPath, partitionsCriteria)
          case LoadMode.OverwritePartitionsWithAddedColumns =>
            val existingDf = format.read(df.sparkSession.read, finalLocation)
            val outputDf = df.addMissingColumns(existingDf.schema)
            loadPartitions(fs, outputDf, finalPath, tempDataPath, tempBackupPath, partitionsCriteria)
          case LoadMode.AppendJoinPartitions =>
            val isRequiredPartition = DataFrameUtils.buildPartitionsCriteriaMatcherFunc(partitionsCriteria, df.schema)
            val existingDf = format.read(df.sparkSession.read, finalLocation).filter(isRequiredPartition)
            val joinColumns = existingDf.columns.toSet intersect df.columns.toSet
            val combinedDf = existingDf.join(df, joinColumns.toSeq, "FULL_OUTER")
            loadPartitions(fs, combinedDf, finalPath, tempDataPath, tempBackupPath, partitionsCriteria)
          case LoadMode.AppendUnionPartitions =>
            val isRequiredPartition = DataFrameUtils.buildPartitionsCriteriaMatcherFunc(partitionsCriteria, df.schema)
            val existingDf = format.read(df.sparkSession.read, finalLocation).filter(isRequiredPartition)
            val combinedDf = df.addMissingColumns(existingDf.schema).union(existingDf)
            loadPartitions(fs, combinedDf, finalPath, tempDataPath, tempBackupPath, partitionsCriteria)
        }

        fs.delete(tempPath, true)
      } match {
        case Failure(exception) => throw exception
        case Success(_) => df
      }

    }

    private def write(fs: FileSystem, df: DataFrame, finalPath: Path, loadMode: LoadMode): DataFrame = {
      Try {
          val writer = getWriter(df).options(options).mode(loadMode.sparkMode)
          format.write(writer, finalPath.toUri.toString)
          logger.info(s"Data was successfully written to $finalPath")
      } match {
          case Failure(exception) => throw new RuntimeException("Unable to process data", exception)
          case Success(_) => df
      }
    }

    private def loadTable(fs: FileSystem, df: DataFrame, finalPath: Path, dataPath: Path, backupPath: Path): Unit = {
      write(fs, df, dataPath, LoadMode.OverwriteTable)
      HadoopLoadHelper.backupDirectoryContent(fs, finalPath, backupPath)

      logger.info(s"Loading data to final location $finalPath")
      try {
        HadoopLoadHelper.loadTable(fs, dataPath, finalPath)
      } catch {
        case e: Throwable =>
          logger.error("Data processing failed", e)
          logger.info(s"Restoring previous state $backupPath -> $finalPath")
          HadoopLoadHelper.restoreDirectoryContent(fs, backupPath, finalPath)
          throw new RuntimeException("Unable to load table", e)
      }
    }

    private def loadPartitions(fs: FileSystem, df: DataFrame, finalPath: Path, dataPath: Path, backupPath: Path,
                               partitionsCriteria: Seq[Seq[(String, String)]]): Unit = {
      if (partitionsCriteria.nonEmpty) {
        write(fs, df, dataPath, LoadMode.OverwritePartitionsWithAddedColumns)

        logger.info(s"Creating backup in $backupPath")
        val backupSpecs = HadoopLoadHelper.createMoveSpecs(fs, finalPath, backupPath, partitionsCriteria)
        HadoopLoadHelper.backupPartitions(fs, backupSpecs)

        logger.info(s"Loading data to final location $finalPath")
        val loadSpecs = HadoopLoadHelper.createMoveSpecs(fs, dataPath, finalPath, partitionsCriteria)

        try {
          HadoopLoadHelper.loadPartitions(fs, loadSpecs)
        } catch {
          case e: Throwable =>
            logger.error("Data processing failed", e)
            logger.info(s"Restoring previous state $backupPath -> $finalPath")
            HadoopLoadHelper.restorePartitions(fs, backupSpecs)
            throw new RuntimeException(s"Unable to load data to $finalPath", e)
        }
      } else {
        logger.warn(s"Unable to load data, output data has no partitions for partition columns $targetPartitions")
      }
    }
  }

  case class TableWriter(table: String, targetPartitions: Seq[String], options: Map[String, String],
                         loadMode: LoadMode) extends OutputWriter {

    override def write(dfs: DFSWrapper, df: DataFrame): DataFrame = {
      Try {
        logger.info(s"Writing data to table $table")
        if (loadMode == LoadMode.OverwriteTable) {
          val spark = df.sparkSession
          spark.sql(s"TRUNCATE TABLE $table")
        }
        getWriter(df).options(options).mode(loadMode.sparkMode).saveAsTable(table)
      } match {
        case Failure(exception) => throw exception
        case Success(_) => df
      }

    }
  }

  case class FileSystemWriter(location: String, format: DataFormat, targetPartitions: Seq[String],
                              options: Map[String, String], loadMode: LoadMode) extends AtomicWriter {

    override def write(dfs: DFSWrapper, df: DataFrame): DataFrame = {
      writeUnsafe(dfs, df, location, loadMode)
    }

    override def writeWithBackup(dfs: DFSWrapper, df: DataFrame): DataFrame = {
      writeSafe(dfs, df, location, loadMode)
    }
  }

  case class TableLocationWriter(table: String, format: DataFormat, targetPartitions: Seq[String],
                                 options: Map[String, String], loadMode: LoadMode,
                                 metadataConfiguration: Metadata) extends AtomicWriter {

    override def write(dfs: DFSWrapper, df: DataFrame): DataFrame = {
      val spark = df.sparkSession
      val location = getTableLocation(spark)
      writeUnsafe(dfs, df, location, loadMode)
      updatePartitionsMetadata(df)
    }

    override def writeWithBackup(dfs: DFSWrapper, df: DataFrame): DataFrame = {
      val spark = df.sparkSession
      val location = getTableLocation(spark)
      writeSafe(dfs, df, location, loadMode)
      updatePartitionsMetadata(df)
    }

    private def updatePartitionsMetadata(df: DataFrame): DataFrame = {
      Try {
        if (targetPartitions.nonEmpty) {
          metadataConfiguration.recoverPartitions(df)
        } else {
          metadataConfiguration.refreshTable(df)
        }
      } match {
        case Failure(exception) => throw exception
        case Success(_) => df
      }
    }

    private def getTableLocation(spark: SparkSession): String = {
      HiveTableAttributeReader(table, spark).getTableLocation
    }
  }
}
