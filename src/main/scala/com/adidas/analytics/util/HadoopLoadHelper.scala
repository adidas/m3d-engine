package com.adidas.analytics.util

import java.io.IOException

import com.adidas.analytics.util.DFSWrapper._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.joda.time.LocalDateTime
import org.slf4j.{Logger, LoggerFactory}


object HadoopLoadHelper {

  private val logger: Logger = LoggerFactory.getLogger(getClass)


  def buildTempPath(directoryPath: Path): Path = {
    val dateTime = LocalDateTime.now().toString("yyyyMMdd_HHmm")
    new Path(directoryPath.getParent, s"${directoryPath.getName}_tmp_$dateTime")
  }

  def cleanupDirectoryContent(fs: FileSystem, dir: Path): Unit = {
    logger.info(s"Cleaning up location $dir")
    try {
      fs.deleteAll(fs.listStatus(dir).map(_.getPath), recursive = true)
      logger.info("Cleanup successfully completed")
    } catch {
      case e: Throwable => throw new RuntimeException(s"Unable to cleanup directory $dir", e)
    }
  }

  def cleanupDirectoryContent(fs: FileSystem, dirString: String): Unit = {
    val dir = new Path(dirString)
    cleanupDirectoryContent(fs, dir)
  }

  def backupDirectoryContent(fs: FileSystem, sourceDir: Path, backupDir: Path): Unit = {
    logger.info(s"Creating backup $sourceDir -> $backupDir")
    try {
      moveChildren(fs, sourceDir, backupDir)
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

  def restoreDirectoryContent(fs: FileSystem, sourceDir: Path, backupDir: Path): Unit = {
    logger.info(s"Restoring directory state $backupDir -> $sourceDir")
    try {
      cleanupDirectoryContent(fs, sourceDir)
      moveChildren(fs, backupDir, sourceDir)
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

  def createMoveSpecs(fs: FileSystem, sourceDir: Path, targetDir: Path, partitionsCriteria: Seq[Seq[(String, String)]]): Seq[MoveSpec] = {
    partitionsCriteria.map { partitionCriteria =>
      val subdirectories = DataFrameUtils.mapPartitionsToDirectories(partitionCriteria)
      MoveSpec(sourceDir.join(subdirectories), targetDir.join(subdirectories))
    }.filter(spec => fs.exists(spec.source))
  }

  def backupPartitions(fs: FileSystem, backupSpecs: Seq[MoveSpec]): Unit = {
    try {
      moveDirectories(fs, backupSpecs)
      fs.deleteAll(backupSpecs.map(_.source), recursive = true)
    } catch {
      case e: Throwable => throw new RuntimeException("Unable to backup partitions", e)
    }
  }

  def restorePartitions(fs: FileSystem, backupSpecs: Seq[MoveSpec]): Unit = {
    try {
      val restoreSpecs = backupSpecs.map(_.reverse)
      fs.deleteAll(restoreSpecs.map(_.target), recursive = true)
      moveDirectories(fs, restoreSpecs)
    } catch {
      case e: Throwable => throw new RuntimeException("Unable to restore partitions", e)
    }
  }

  def loadTable(fs: FileSystem, sourceDir: Path, targetDir: Path): Unit = {
    try {
      moveChildren(fs, sourceDir, targetDir)
    } catch {
      case e: Throwable => throw new RuntimeException("Unable to load table", e)
    }
  }

  def loadPartitions(fs: FileSystem, loadSpecs: Seq[MoveSpec]): Unit = {
    try {
      moveDirectories(fs, loadSpecs)
    } catch {
      case e: Throwable => throw new RuntimeException("Unable to load partitions", e)
    }
  }

  private def moveChildren(fs: FileSystem, sourceDir: Path, targetDir: Path): Unit = {
    try {
      val childObjects = fs.listStatus(sourceDir).map(_.getPath)
      fs.renameAll(childObjects, targetDir)
    } catch {
      case e: Throwable => throw new IOException(s"Unable to move directory content $sourceDir -> $targetDir", e)
    }
  }

  private def moveDirectories(fs: FileSystem, specs: Seq[MoveSpec]): Unit = {
    specs.par.foreach { spec =>
      logger.info(s"Moving partition directory ${spec.source} -> ${spec.target}")
      if (fs.exists(spec.target) || !fs.mkdirs(spec.target)) {
        throw new IOException(s"Unable to create target directory ${spec.target}")
      }
      moveChildren(fs, spec.source, spec.target)
    }
  }

  protected case class MoveSpec(source: Path, target: Path) {
    def reverse: MoveSpec = MoveSpec(target, source)
  }
}
