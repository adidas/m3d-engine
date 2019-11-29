package com.adidas.utils

import java.util.UUID

import com.adidas.analytics.util.{DFSWrapper, LoadMode}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.StructType
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}
import org.slf4j.{Logger, LoggerFactory}

import scala.io.Source

trait BaseAlgorithmTest extends Suite with BeforeAndAfterAll with BeforeAndAfterEach with HDFSSupport with SparkSupport {

  override val logger: Logger = LoggerFactory.getLogger(getClass)
  override val testAppId: String = UUID.randomUUID().toString
  override val localTestDir: String = "target"
  override val sparkHadoopConf: Option[Configuration] = Some(fs.getConf)

  val hdfsRootTestPath: Path = new Path("hdfs:///tmp/tests")
  val dfs: DFSWrapper = DFSWrapper(spark.sparkContext.hadoopConfiguration)

  override def afterAll(): Unit = {
    spark.stop()
    cluster.shutdown(true)
  }

  override def beforeEach(): Unit = {
    fs.delete(hdfsRootTestPath, true)
    fs.mkdirs(hdfsRootTestPath)
  }

  override def afterEach(): Unit = {
    spark.sqlContext.clearCache()
    spark.sparkContext.getPersistentRDDs.foreach {
      case (_, rdd) => rdd.unpersist(true)
    }
  }

  def resolveResource(fileName: String, withProtocol: Boolean = false): String = {
    val resource = s"${getClass.getSimpleName}/$fileName"
    logger.info(s"Resolving resource $resource")
    val location = getClass.getClassLoader.getResource(resource).getPath
    if (withProtocol) {
      s"file://$location"
    } else {
      location
    }
  }

  def getResourceAsText(fileName: String): String = {
    val resource = s"${getClass.getSimpleName}/$fileName"
    logger.info(s"Reading resource $resource")
    val stream = getClass.getClassLoader.getResourceAsStream(resource)
    Source.fromInputStream(stream).mkString
  }

  def copyResourceFileToHdfs(resource: String, targetPath: Path): Unit = {
    val localResourceRoot = resolveResource("", withProtocol = true)
    val sourcePath = new Path(localResourceRoot, resource)
    logger.info(s"Copying local resource to HDFS $sourcePath -> $targetPath")
    fs.copyFromLocalFile(sourcePath, targetPath)
  }

  /*
   * Creates (but does not load) a Parquet table for testing purposes
   */
  def createParquetTable(database: String, tableName: String, partitionColumns: Option[Seq[String]] = None, schema: StructType): Table = {
    val inputTableLocation = fs.makeQualified(new Path(hdfsRootTestPath, s"$database/$tableName")).toString
    if (partitionColumns.isEmpty)
      Table.newBuilder(tableName, database, inputTableLocation, schema)
        .buildParquetTable(DFSWrapper(fs.getConf), spark, external = true)
    else
      Table.newBuilder(tableName, database, inputTableLocation, schema)
        .withPartitions(partitionColumns.get)
        .buildParquetTable(DFSWrapper(fs.getConf), spark, external = true)
  }

  /*
   * Creates and Loads Parquet table for testing purposes
   */
  def createAndLoadParquetTable(database: String, tableName: String, partitionColumns: Option[Seq[String]] = None, schema: StructType, filePath: String, reader: FileReader): Table = {
    val table = createParquetTable(database, tableName, partitionColumns, schema)
    val inputTableDataURI = resolveResource(filePath, withProtocol = true)
    table.write(Seq(inputTableDataURI), reader, LoadMode.OverwritePartitions)
    table
  }
}
