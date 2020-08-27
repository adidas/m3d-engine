package com.adidas.analytics.util

import java.io.IOException
import com.adidas.analytics.util.DFSWrapper._
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path, PathFilter}
import org.joda.time.LocalDateTime
import org.slf4j.{Logger, LoggerFactory}
import scala.util.{Failure, Success, Try}

object HadoopLoadHelper {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def buildTimestampedTablePath(directoryPath: Path): Path = {
    val dateTime = LocalDateTime.now().toString("yyyyMMddHHmmssSSS")
    new Path(directoryPath.getParent, s"${directoryPath.getName}_$dateTime")
  }

  def buildUTCTimestampedTablePath(directoryPath: Path): Path = {
    val dateTime = s"${LocalDateTime.now().toString("yyyyMMdd_HHmmss")}_UTC"
    new Path(directoryPath, s"$dateTime")
  }

  def buildTempPath(directoryPath: Path): Path = {
    val dateTime = LocalDateTime.now().toString("yyyyMMdd_HHmm")
    new Path(directoryPath.getParent, s"${directoryPath.getName}_tmp_$dateTime")
  }

  def cleanupDirectoryContent(fs: FileSystem, dir: Path): Unit = {
    logger.info(s"Cleaning up location $dir")
    try if (fs.exists(dir)) {
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

  def cleanupDirectoryContent(dfs: DFSWrapper, dirString: String): Unit = {
    val dir = new Path(dirString)
    val fs = dfs.getFileSystem(dir)
    cleanupDirectoryContent(fs, dir)
  }

  def cleanupDirectoryLeftovers(fs: FileSystem, dir: Path, ignorePrefixes: Seq[String]): Unit = {
    logger.info(s"Cleaning up leftovers in directory $dir")
    try if (fs.exists(dir)) {
      fs.deleteAll(
        fs.listStatus(dir)
          .map(_.getPath)
          .filter(path => !ignorePrefixes.exists(path.toString.contains)),
        recursive = true
      )
      logger.info("Cleaning up leftovers successfully completed")
    } else logger.info(s"Folder $dir did not exist! Cleanup of leftovers skipped")
    catch {
      case e: Throwable =>
        throw new RuntimeException(s"Unable to cleanup leftovers in directory $dir", e)
    }
  }

  def cleanupDirectoryLeftovers(dfs: DFSWrapper, dirString: String, ignorePrefix: String): Unit = {
    val dir = new Path(dirString)
    val fs = dfs.getFileSystem(dir)
    cleanupDirectoryLeftovers(fs, dir, Seq(ignorePrefix))
  }

  def cleanupDirectoryLeftovers(
      dfs: DFSWrapper,
      dirString: String,
      ignorePrefixes: Seq[String]
  ): Unit = {
    val dir = new Path(dirString)
    val fs = dfs.getFileSystem(dir)
    cleanupDirectoryLeftovers(fs, dir, ignorePrefixes)
  }

  /** Gets a list of the ordered subfolders in a specific folder.
    *
    * @param dfs
    *   distributed file system
    * @param dir
    *   directory to check for subfolders
    * @param maxResults
    *   Optional number of subfolders to get
    * @param fileFilter
    *   optional PathFilter to avoid considering some folders/files in the ordering process (e.g.,
    *   S3 EMR folder placeholders, partition folders)
    * @param ordering
    *   type of string ordering
    * @return
    *   list of ordered subfolders
    */
  def getOrderedSubFolders(
      dfs: DFSWrapper,
      dir: String,
      maxResults: Option[Int] = None,
      fileFilter: Option[PathFilter] = None,
      ordering: Ordering[String] = Ordering.String.reverse
  ): Seq[String] = {
    val tableParentDirPath = new Path(dir)
    val fs = dfs.getFileSystem(tableParentDirPath)

    val files = {
      if (fileFilter.isDefined) fs.listStatus(tableParentDirPath, fileFilter.get)
      else fs.listStatus(tableParentDirPath)
    }

    val prefixes = files.map(_.getPath.getName).toSeq.sorted(ordering)

    if (maxResults.isDefined) prefixes.take(maxResults.get) else prefixes
  }

  def backupDirectoryContent(
      fs: FileSystem,
      sourceDir: Path,
      backupDir: Path,
      move: Boolean = true
  ): Unit = {
    logger.info(s"Creating backup $sourceDir -> $backupDir")
    try {
      if (move) {
        if (fs.exists(backupDir) || !fs.mkdirs(backupDir))
          throw new IOException(s"Unable to create target directory $backupDir")
        moveChildren(fs, sourceDir, backupDir)
      } else copyChildren(fs, sourceDir, backupDir)
      logger.info("Backup successfully created")
    } catch {
      case e: Throwable => throw new RuntimeException(s"Unable to backup content of $sourceDir", e)
    }
  }

  def backupDirectoryContent(
      dfs: DFSWrapper,
      sourceDirString: String,
      backupDirString: String,
      move: Boolean
  ): Unit = {
    val sourceDir = new Path(sourceDirString)
    val backupDir = new Path(backupDirString)
    val fs = dfs.getFileSystem(sourceDir)
    backupDirectoryContent(fs, sourceDir, backupDir, move)
  }

  def restoreDirectoryContent(
      fs: FileSystem,
      sourceDir: Path,
      backupDir: Path,
      move: Boolean = true
  ): Unit = {
    logger.info(s"Restoring directory state $backupDir -> $sourceDir")
    try {
      cleanupDirectoryContent(fs, sourceDir)
      if (move) moveChildren(fs, backupDir, sourceDir) else copyChildren(fs, backupDir, sourceDir)
      logger.info("Previous state was successfully restored")
    } catch {
      case e: Throwable => throw new RuntimeException(s"Unable to restore state of $sourceDir", e)
    }
  }

  def restoreDirectoryContent(
      dfs: DFSWrapper,
      sourceDirString: String,
      backupDirString: String,
      move: Boolean
  ): Unit = {
    val sourcePath = new Path(sourceDirString)
    val backupPath = new Path(backupDirString)
    val fs = dfs.getFileSystem(sourcePath)
    restoreDirectoryContent(fs, sourcePath, backupPath, move)
  }

  def createMoveSpecs(
      fs: FileSystem,
      sourceDir: Path,
      targetDir: Path,
      partitionsCriteria: Seq[Seq[(String, String)]]
  ): Seq[MoveSpec] =
    partitionsCriteria
      .map { partitionCriteria =>
        val subdirectories = DataFrameUtils.mapPartitionsToDirectories(partitionCriteria)
        MoveSpec(sourceDir.join(subdirectories), targetDir.join(subdirectories))
      }
      .filter(spec => fs.exists(spec.source))

  def backupPartitions(fs: FileSystem, backupSpecs: Seq[MoveSpec]): Unit =
    try {
      moveDirectories(fs, backupSpecs)
      fs.deleteAll(backupSpecs.filter(ms => fs.exists(ms.source)).map(_.source), recursive = true)
    } catch { case e: Throwable => throw new RuntimeException("Unable to backup partitions", e) }

  def restorePartitions(fs: FileSystem, backupSpecs: Seq[MoveSpec]): Unit =
    try {
      val restoreSpecs = backupSpecs.map(_.reverse)
      fs.deleteAll(restoreSpecs.filter(ms => fs.exists(ms.target)).map(_.target), recursive = true)
      moveDirectories(fs, restoreSpecs)
    } catch { case e: Throwable => throw new RuntimeException("Unable to restore partitions", e) }

  def loadTable(fs: FileSystem, sourceDir: Path, targetDir: Path): Unit =
    try moveChildren(fs, sourceDir, targetDir)
    catch { case e: Throwable => throw new RuntimeException("Unable to load table", e) }

  def loadPartitions(fs: FileSystem, loadSpecs: Seq[MoveSpec]): Unit =
    try moveDirectories(fs, loadSpecs)
    catch { case e: Throwable => throw new RuntimeException("Unable to load partitions", e) }

  private def moveChildren(fs: FileSystem, sourceDir: Path, targetDir: Path): Unit =
    try {
      val childObjects = fs.listStatus(sourceDir).map(_.getPath)
      fs.renameAll(childObjects, targetDir)
    } catch {
      case e: Throwable =>
        throw new IOException(s"Unable to move directory content $sourceDir -> $targetDir", e)
    }

  def copyChildren(fs: FileSystem, sourceDir: Path, targetDir: Path): Unit =
    Try {
      fs.listStatus(sourceDir).foreach { file =>
        FileUtil
          .copy(fs, file, fs, targetDir.join(Seq(file.getPath.getName)), false, true, fs.getConf)
      }
    } match {
      case Failure(e) =>
        throw new IOException(s"Unable to copy directory content $sourceDir -> $targetDir", e)
      case Success(_) =>
    }

  private def moveDirectories(fs: FileSystem, specs: Seq[MoveSpec]): Unit =
    specs.par.foreach { spec =>
      logger.info(s"Moving partition directory ${spec.source} -> ${spec.target}")
      if (fs.exists(spec.target) || !fs.mkdirs(spec.target))
        throw new IOException(s"Unable to create target directory ${spec.target}")
      moveChildren(fs, spec.source, spec.target)
    }

  protected case class MoveSpec(source: Path, target: Path) {
    def reverse: MoveSpec = MoveSpec(target, source)
  }

}
