package com.adidas.analytics.algo.core

import com.adidas.analytics.algo.core.Algorithm.{BaseReadOperation, BaseWriteOperation, _}
import com.adidas.analytics.util.OutputWriter.AtomicWriter
import com.adidas.analytics.util.{DFSWrapper, InputReader, OutputWriter}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/** Base trait for algorithms that defines their base methods
  */
trait Algorithm
    extends JobRunner
    with Serializable
    with BaseReadOperation
    with BaseWriteOperation
    with BaseUpdateStatisticsOperation {

  protected def spark: SparkSession

  protected def dfs: DFSWrapper

  /** A function which is supposed to have DataFrame transformations, its implementation is optional
    *
    * @param dataFrames
    *   input DataFrame
    * @return
    *   modified DataFrame
    */
  protected def transform(dataFrames: Vector[DataFrame]): Vector[DataFrame] = dataFrames

  /** The main entry point for running the algorithm
    */
  override def run(): Unit = {
    logger.info("Starting reading stage...")
    val inputDateFrames = read()
    logger.info("Starting processing stage...")
    val result = transform(inputDateFrames)
    logger.info("Starting writing stage...")
    val outputResult = write(result)
    logger.info("Starting computing statistics...")
    updateStatistics(outputResult)
  }
}

object Algorithm {

  private val logger: Logger = LoggerFactory.getLogger(this.getClass)

  /** Base trait for read operations
    */
  trait BaseReadOperation {

    /** Reads a DataFrame using logic defined in the inheritor class
      *
      * @return
      *   DataFrame which was read
      */
    protected def read(): Vector[DataFrame]
  }

  /** Base trait for update statistics operations
    */
  trait BaseUpdateStatisticsOperation {

    /** Reads the produced output dataframe and update table statistics
      *
      * @return
      *   DataFrame written in writer() step
      */
    protected def updateStatistics(dataFrames: Vector[DataFrame]): Unit
  }

  /** The simplest implementation of update statistics
    */
  trait UpdateStatisticsOperation extends BaseUpdateStatisticsOperation {

    /** By default the Update Statistics are disabled for a given Algorithm
      * @param dataFrames
      *   Dataframes to compute statistics
      */
    override protected def updateStatistics(dataFrames: Vector[DataFrame]): Unit =
      logger.info("Skipping update statistics step!")

  }

  /** Base trait for write operations
    */
  trait BaseWriteOperation {

    /** Defines a number of output partitions
      *
      * @return
      *   number of output partitions
      */
    protected def outputFilesNum: Option[Int] = None

    /** Writes the DataFrame using logic defined in the inheritor class
      *
      * @param dataFrames
      *   DataFrame to write
      */
    protected def write(dataFrames: Vector[DataFrame]): Vector[DataFrame]
  }

  /** Simple implementation of read operation. It just reads data using a reader which is defined in
    * the inheritor class
    */
  trait ReadOperation extends BaseReadOperation {

    protected def spark: SparkSession

    /** Defines a reader which is used for reading data
      *
      * @return
      *   An implementation of InputReader
      */
    protected def readers: Vector[InputReader]

    override protected def read(): Vector[DataFrame] = readers.map(_.read(spark))
  }

  /** Implementation of write operation that uses a writer which is defined in the inheritor class
    * for writing data to the file system in an atomic way
    */
  trait SafeWriteOperation extends BaseWriteOperation {

    protected def dfs: DFSWrapper

    /** Defines a writer which is used for writing data
      *
      * @return
      *   An implementation of AtomicWriter which support writing data in atomic way
      */
    protected def writer: AtomicWriter

    override protected def write(dataFrames: Vector[DataFrame]): Vector[DataFrame] =
      dataFrames.map { df =>
        writer.writeWithBackup(dfs, outputFilesNum.map(df.repartition).getOrElse(df))
      }
  }

  /** Simple implementation of write operation. It just writes data using a writer which is defined
    * in the inheritor class
    */
  trait WriteOperation extends BaseWriteOperation {

    protected def dfs: DFSWrapper

    /** Defines a writer which is used for writing data
      *
      * @return
      *   An implementation of OutputWriter
      */
    protected def writer: OutputWriter

    override protected def write(dataFrames: Vector[DataFrame]): Vector[DataFrame] =
      dataFrames.map(df => writer.write(dfs, outputFilesNum.map(df.repartition).getOrElse(df)))
  }

}
