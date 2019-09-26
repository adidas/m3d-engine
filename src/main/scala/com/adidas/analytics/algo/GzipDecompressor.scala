package com.adidas.analytics.algo

import java.util.concurrent.{Executors, TimeUnit}

import com.adidas.analytics.algo.GzipDecompressor.{changeFileExtension, compressedExtension, _}
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

/**
  * Spark Scala code utility that can decompress the gzip files and put
  * them in the same location into hadoop distributed file systems.
  * This will be utilised as preliminary activity before spark code execution
  * is being called on these files. As gzip compressed files are not splittable,
  * spark parallel processing can not be used efficiently for these files.
  * This utility will help to uncompress files at runtime of a workflow.
  */
final class GzipDecompressor protected(val spark: SparkSession, val dfs: DFSWrapper, val configLocation: String)
  extends JobRunner with GzipDecompressorConfiguration {

  private val hadoopConfiguration: Configuration = spark.sparkContext.hadoopConfiguration
  private val fileSystem: FileSystem = dfs.getFileSystem(inputDirectoryPath)


  override def run(): Unit = {
    //check if directory exists
    if (!fileSystem.exists(inputDirectoryPath)){
      logger.error(s"Input directory: $inputDirectoryPath does not exist.")
      throw new RuntimeException(s"Directory $inputDirectoryPath does not exist.")
    }

    val compressedFilePaths = fileSystem.ls(inputDirectoryPath, recursive)
      .filterNot(path => fileSystem.isDirectory(path))
      .filter(_.getName.toLowerCase.endsWith(compressedExtension))

    if (compressedFilePaths.isEmpty) {
      logger.warn(s"Input directory $inputDirectoryPath does not contain compressed files. Skipping...")
    } else {
      implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(threadPoolSize))
      Await.result(Future.sequence(
        compressedFilePaths.map { compressedFilePath =>
          Future {
            logger.info(s"Decompressing file: $compressedFilePath")

            val decompressedFileName = changeFileExtension(compressedFilePath.getName, compressedExtension, outputExtension)
            val decompressedFilePath = new Path(compressedFilePath.getParent, decompressedFileName)

            val compressionCodecFactory = new CompressionCodecFactory(hadoopConfiguration)
            val inputCodec = compressionCodecFactory.getCodec(compressedFilePath)

            val inputStream = inputCodec.createInputStream(fileSystem.open(compressedFilePath))
            val output = fileSystem.create(decompressedFilePath)

            IOUtils.copyBytes(inputStream, output, hadoopConfiguration)
            logger.info(s"Finished decompressing file: $compressedFilePath")

            //Delete the compressed file
            fileSystem.delete(compressedFilePath, false)
            logger.info(s"Removed file: $compressedFilePath")
          }
        }
      ), Duration(4, TimeUnit.HOURS))
    }
  }
}


object GzipDecompressor {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private val compressedExtension: String = ".gz"

  def apply(spark: SparkSession, dfs: DFSWrapper, configLocation: String): GzipDecompressor = {
    new GzipDecompressor(spark, dfs, configLocation)
  }

  private def changeFileExtension(fileName: String, currentExt: String, newExt: String): String = {
    val newFileName =  fileName.substring(0, fileName.lastIndexOf(currentExt))
    if (newFileName.endsWith(newExt)) newFileName else newFileName + newExt
  }
}
