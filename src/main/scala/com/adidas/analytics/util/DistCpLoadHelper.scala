package com.adidas.analytics.util

import java.io.IOException

import com.adidas.analytics.util.DFSWrapper._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.LocalDateTime
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}


object DistCpLoadHelper {

  private val logger: Logger = LoggerFactory.getLogger(getClass)


  def buildTempPath(directoryPath: Path): Path = {
    val dateTime = LocalDateTime.now().toString("yyyyMMdd_HHmm")
    new Path(directoryPath.getParent, s"${directoryPath.getName}_tmp_$dateTime")
  }

  def cleanupDirectoryContent(fs: FileSystem, dir: Path): Unit = {
    logger.info(s"Cleaning up location $dir")
    try {
      val childObjects = fs.listStatus(dir).map(_.getPath)
      fs.deleteAll(childObjects, recursive = true)
      logger.info("Cleanup successfully completed")
    } catch {
      case e: Throwable => throw new RuntimeException(s"Unable to cleanup directory $dir", e)
    }
  }

  def cleanupDirectoryContent(fs: FileSystem, dirString: String): Unit = {
    val dir = new Path(dirString)
    cleanupDirectoryContent(fs, dir)
  }

  def cleanupDirectoryContent(dfs: DFSWrapper, dirString: String): Unit = {
    val dir = new Path(dirString)
    val fs = dfs.getFileSystem(dir)
    cleanupDirectoryContent(fs, dir)
  }

  def backupDirectoryContent(fs: FileSystem, sourceDir: Path, backupDir: Path): Unit = {
    logger.info(s"Creating backup $sourceDir -> $backupDir")
    try {
      copyChildren(fs, sourceDir, backupDir)
      logger.info("Backup successfully created")
    } catch {
      case e: Throwable => throw new RuntimeException(s"Unable to backup content of $sourceDir", e)
    }
  }

  def backupDirectoryContent(fs: FileSystem, sourceDirString: String, backupDirString: String): Unit = {
    val sourceDir = new Path(sourceDirString)
    val backupDir = new Path(backupDirString)
    backupDirectoryContent(fs, sourceDir, backupDir)
  }

  def backupDirectoryContent(dfs: DFSWrapper, sourceDirString: String, backupDirString: String): Unit = {
    val sourceDir = new Path(sourceDirString)
    val backupDir = new Path(backupDirString)
    val fs = dfs.getFileSystem(sourceDir)
    backupDirectoryContent(fs, sourceDir, backupDir)
  }

  def restoreDirectoryContent(fs: FileSystem, sourceDir: Path, backupDir: Path): Unit = {
    logger.info(s"Restoring directory state $backupDir -> $sourceDir")
    try {
      cleanupDirectoryContent(fs, sourceDir)
      copyChildren(fs, backupDir, sourceDir)
      logger.info("Previous state was successfully restored")
    } catch {
      case e: Throwable => throw new RuntimeException(s"Unable to restore state of $sourceDir", e)
    }
  }

  def restoreDirectoryContent(fs: FileSystem, sourceDirString: String, backupDirString: String): Unit = {
    val sourcePath = new Path(sourceDirString)
    val backupPath = new Path(backupDirString)
    restoreDirectoryContent(fs, sourcePath, backupPath)
  }

  def restoreDirectoryContent(dfs: DFSWrapper, sourceDirString: String, backupDirString: String): Unit = {
    val sourcePath = new Path(sourceDirString)
    val backupPath = new Path(backupDirString)
    val fs = dfs.getFileSystem(sourcePath)
    restoreDirectoryContent(fs, sourcePath, backupPath)
  }

  def createCopySpecs(fs: FileSystem, sourceDir: Path, targetDir: Path, partitionsCriteria: Seq[Seq[(String, String)]]): Seq[DistCpSpec] = {
    partitionsCriteria.map { partitionCriteria =>
      val subdirectories = DataFrameUtils.mapPartitionsToDirectories(partitionCriteria)
      DistCpSpec(sourceDir.join(subdirectories), targetDir.join(subdirectories))
    }.filter(spec => fs.exists(spec.source))
  }

  def backupPartitions(fs: FileSystem, backupSpecs: Seq[DistCpSpec]): Unit = {
    try {
      copyDirectories(fs, backupSpecs)
      fs.deleteAll(backupSpecs.map(_.source), recursive = true)
    } catch {
      case e: Throwable => throw new RuntimeException("Unable to backup partitions", e)
    }
  }

  def restorePartitions(fs: FileSystem, backupSpecs: Seq[DistCpSpec]): Unit = {
    try {
      copyDirectories(fs, backupSpecs.map(spec => DistCpSpec(spec.target, spec.source)))
    } catch {
      case e: Throwable => throw new RuntimeException("Unable to restore partitions", e)
    }
  }

  def loadPartitions(fs: FileSystem, loadSpecs: Seq[DistCpSpec]): Unit = {
    try {
      copyDirectories(fs, loadSpecs)
    } catch {
      case e: Throwable => throw new RuntimeException("Unable to load partitions", e)
    }
  }

  def copyChildren(fs: FileSystem, sourceDir: Path, targetDir: Path): Unit = {
    Try {
      DistCpWrapper(fs.getConf, Seq(sourceDir), targetDir).run(overwrite = true)
    } match {
      case Failure(e) => throw new IOException(s"Unable to copy directory content $sourceDir -> $targetDir", e)
      case Success(_) =>
    }
  }

  private def copyDirectories(fs: FileSystem, specs: Seq[DistCpSpec]): Unit = {
    specs.foreach { spec =>
      logger.info(s"Copying partition directory ${spec.source} -> ${spec.target}")
      if (fs.exists(spec.target) || !fs.mkdirs(spec.target)) {
        throw new IOException(s"Unable to create target directory ${spec.target}")
      }
      copyChildren(fs, spec.source, spec.target)
    }
  }

  protected case class DistCpSpec(source: Path, target: Path)
}
