package com.adidas.analytics.util

import java.io.{IOException, ObjectInputStream, ObjectOutputStream, PrintWriter}
import com.adidas.analytics.util.DFSWrapper.ConfigurationWrapper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.mutable
import scala.io.Source
import scala.util.{Failure, Success, Try}

class DFSWrapper private (config: ConfigurationWrapper) extends Serializable {

  @transient
  private val fsCache: mutable.Map[String, FileSystem] = mutable.Map.empty

  def getFileSystem(path: Path): FileSystem =
    fsCache.getOrElseUpdate(path.toUri.getHost, path.getFileSystem(config.hadoopConfiguration))
}

object DFSWrapper {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def apply(hadoopConfiguration: Configuration): DFSWrapper =
    new DFSWrapper(new ConfigurationWrapper(hadoopConfiguration))

  final class ConfigurationWrapper(
      @transient
      var hadoopConfiguration: Configuration
  ) extends Serializable {

    //noinspection ScalaUnusedSymbol
    private def writeObject(out: ObjectOutputStream): Unit = hadoopConfiguration.write(out)

    //noinspection ScalaUnusedSymbol
    private def readObject(in: ObjectInputStream): Unit = {
      hadoopConfiguration = new Configuration(false)
      hadoopConfiguration.readFields(in)
    }
  }

  implicit class ExtendedFileSystem(fs: FileSystem) {

    def writeFile(path: Path, content: String): Unit = {
      val outStream = fs.create(path)
      val writer = new PrintWriter(outStream)
      writer.write(content)
      writer.close()
    }

    def readFile(path: Path): String = Source.fromInputStream(fs.open(path)).mkString

    def ls(inputPath: Path, recursive: Boolean = false): Seq[Path] = {
      import RemoteIteratorWrapper._
      fs.listFiles(inputPath, recursive).remoteIteratorToIterator.map(_.getPath).toVector
    }

    def deleteAll(paths: Seq[Path], recursive: Boolean = false): Unit =
      paths.par.foreach { path =>
        Try(fs.delete(path, recursive)) match {
          case Failure(e) =>
            val ex = new IOException(s"Unable to delete $path", e)
            logger.error(ex.getMessage)
            throw new IOException(ex)
          case Success(false) =>
            val ex = new IOException(s"Unable to delete $path")
            logger.error(ex.getMessage)
            throw new IOException(ex)
          case Success(true) =>
        }
      }

    def renameAll(sources: Seq[Path], targetDir: Path): Unit =
      sources.par.foreach { source =>
        val path = new Path(targetDir, source.getName)
        Try(fs.rename(source, path)) match {
          case Failure(e) =>
            val ex = new IOException(s"Unable to move $source -> $path", e)
            logger.error(ex.getMessage)
            throw new IOException(ex)
          case Success(false) =>
            val ex = new IOException(s"Unable to move $source -> $path")
            logger.error(ex.getMessage)
            throw new IOException(ex)
          case Success(true) =>
        }
      }

    def createDirIfNotExists(path: Path): Unit =
      if (!fs.exists(path)) Try(fs.mkdirs(path)) match {
        case Failure(e) =>
          val ex = new IOException(s"Unable to create $path", e)
          logger.error(ex.getMessage)
          throw new IOException(ex)
        case Success(false) =>
          val ex = new IOException(s"Unable to create $path")
          logger.error(ex.getMessage)
          throw new IOException(ex)
        case Success(true) =>
      }
  }

  implicit class ExtendedPath(path: Path) {

    def join(children: Seq[String]): Path =
      children.foldLeft(path)((parentPath, suffix) => new Path(parentPath, suffix))
  }

}
