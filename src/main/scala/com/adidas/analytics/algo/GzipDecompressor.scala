package com.adidas.analytics.algo

import java.util.concurrent.{Executors, TimeUnit}
import java.util.zip.ZipInputStream
import com.adidas.analytics.algo.GzipDecompressor.{getDecompressedFilePath, logger}
import com.adidas.analytics.algo.core.JobRunner
import com.adidas.analytics.config.GzipDecompressorConfiguration
import com.adidas.analytics.util.DFSWrapper
import com.adidas.analytics.util.DFSWrapper._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}
import scala.concurrent._
import scala.concurrent.duration._

/** Spark Scala code utility that can decompress the gzip files and put them in the same location
  * into hadoop distributed file systems. This will be utilised as preliminary activity before spark
  * code execution is being called on these files. As gzip compressed files are not splittable,
  * spark parallel processing can not be used efficiently for these files. This utility will help to
  * uncompress files at runtime of a workflow.
  */
final class GzipDecompressor protected (
    val spark: SparkSession,
    val dfs: DFSWrapper,
    val configLocation: String
) extends JobRunner
    with GzipDecompressorConfiguration {

  private val hadoopConfiguration: Configuration = spark.sparkContext.hadoopConfiguration
  private val fileSystem: FileSystem = dfs.getFileSystem(inputDirectoryPath)

  override def run(): Unit = {
    //check if directory exists
    if (!fileSystem.exists(inputDirectoryPath)) {
      logger.error(s"Input directory: $inputDirectoryPath does not exist.")
      throw new RuntimeException(s"Directory $inputDirectoryPath does not exist.")
    }

    implicit val ec: ExecutionContext =
      ExecutionContext.fromExecutor(Executors.newFixedThreadPool(threadPoolSize))
    Await.result(
      Future.sequence(
        fileSystem
          .ls(inputDirectoryPath, recursive)
          .filterNot(path => fileSystem.getFileStatus(path).isDirectory)
          .map { compressedFilePath =>
            Future {
              val decompressedFilePath =
                getDecompressedFilePath(compressedFilePath, outputExtension)
              val inputStream =
                if (compressedFilePath.getName.endsWith(".zip")) {
                  val zin = new ZipInputStream(fileSystem.open(compressedFilePath))
                  /* Warning: we intentionally only support zip files with one entry here as we want
                   * to control the output name and can not merge multiple entries because they may
                   * have headers. */
                  zin.getNextEntry
                  zin
                } else {
                  val compressionCodecFactory = new CompressionCodecFactory(hadoopConfiguration)
                  val inputCodec = compressionCodecFactory.getCodec(compressedFilePath)
                  if (inputCodec != null)
                    inputCodec.createInputStream(fileSystem.open(compressedFilePath))
                  else {
                    logger.error(s"No codec found for file $compressedFilePath!")
                    throw new RuntimeException(s"No codec found for file $compressedFilePath!")
                  }
                }

              logger.info(s"Decompressing file: $compressedFilePath")
              val outputStream = fileSystem.create(decompressedFilePath)

              IOUtils.copyBytes(inputStream, outputStream, hadoopConfiguration)
              logger.info(s"Finished decompressing file: $compressedFilePath")

              inputStream.close()
              outputStream.close()

              fileSystem.delete(compressedFilePath, false)
              logger.info(s"Removed file: $compressedFilePath")
            }
          }
      ),
      Duration(4, TimeUnit.HOURS)
    )
  }
}

object GzipDecompressor {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  def apply(spark: SparkSession, dfs: DFSWrapper, configLocation: String): GzipDecompressor =
    new GzipDecompressor(spark, dfs, configLocation)

  private def getDecompressedFilePath(compressedFilePath: Path, outputExt: String): Path = {
    val decompressedFileName = compressedFilePath.getName.replaceAll("\\.[^.]*$", ".") + outputExt
    new Path(compressedFilePath.getParent, decompressedFileName)
  }
}
