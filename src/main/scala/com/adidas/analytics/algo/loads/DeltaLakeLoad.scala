package com.adidas.analytics.algo.loads

import com.adidas.analytics.algo.core.{Algorithm, TableStatistics}
import com.adidas.analytics.algo.shared.DateComponentDerivation
import com.adidas.analytics.config.loads.DeltaLakeLoadConfiguration
import com.adidas.analytics.util.DataFormat.{DSVFormat, JSONFormat, ParquetFormat}
import com.adidas.analytics.util.DataFrameUtils._
import com.adidas.analytics.util.{DFSWrapper, DataFormat, DataFrameUtils}
import io.delta.tables._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable

final class DeltaLakeLoad protected (
    val spark: SparkSession,
    val dfs: DFSWrapper,
    val configLocation: String
) extends Algorithm
    with DeltaLakeLoadConfiguration
    with TableStatistics
    with DateComponentDerivation {

  override protected def read(): Vector[DataFrame] =
    try {
      val dataFormat: DataFormat = fileFormat match {
        case "parquet" => ParquetFormat()
        case "dsv"     => DSVFormat()
        case "json" =>
          JSONFormat(multiLine = isMultilineJSON.getOrElse(false), optionalSchema = readJsonSchema)
        case _ => throw new RuntimeException(s"Unsupported input data format $fileFormat.")
      }

      val inputDF = dataFormat.read(spark.read.options(sparkReaderOptions), inputDir)
      Vector(inputDF.select(inputDF.columns.map(c => col(c).as(c.toLowerCase)): _*))
    } catch { case e: Throwable => throw new RuntimeException("Unable to read input data.", e) }

  override protected def transform(dataFrames: Vector[DataFrame]): Vector[Dataset[Row]] =
    try Vector(
      initOrUpdateDeltaTable(withDatePartitions(dataFrames(0))).selectExpr(targetTableColumns: _*)
    )
    catch { case e: Throwable => throw new RuntimeException("Could not update Delta Table!", e) }

  override protected def write(dataFrames: Vector[DataFrame]): Vector[Dataset[Row]] = {
    writer.writeWithBackup(dfs, dataFrames(0), Some(affectedPartitions))
    dataFrames
  }

  /** Check if it is init load or not (i.e., delta table was already loaded at least once) and
    * proceed to create the delta table or merge it with the new arriving data.
    *
    * @param newDataDF
    *   DataFrame containing new data to update the Delta table
    * @return
    *   the DeltaTable object
    */
  private def initOrUpdateDeltaTable(newDataDF: DataFrame): DataFrame =
    if (DeltaTable.isDeltaTable(deltaTableDir)) {
      newDataDF.persist(StorageLevel.MEMORY_AND_DISK)
      affectedPartitions = newDataDF.collectPartitions(targetPartitions)
      val affectedPartitionsFilter =
        DataFrameUtils.buildPartitionsCriteriaMatcherFunc(affectedPartitions, newDataDF.schema)

      val condensedNewDataDF = condenseNewData(newDataDF)
      newDataDF.unpersist()
      condensedNewDataDF.persist(StorageLevel.MEMORY_AND_DISK)

      mergeDeltaTable(condensedNewDataDF)
      condensedNewDataDF.unpersist()

      if (isManualRepartitioning)
        repartitionDeltaTable(
          DeltaTable.forPath(deltaTableDir),
          affectedPartitions,
          affectedPartitionsFilter
        )

      var deltaTableDF = DeltaTable.forPath(deltaTableDir).toDF
      if (targetPartitions.nonEmpty) deltaTableDF = deltaTableDF.filter(affectedPartitionsFilter)

      deltaTableDF
    } else {
      val condensedNewDataDF = condenseNewData(newDataDF, initLoad = true)

      affectedPartitions = condensedNewDataDF.collectPartitions(targetPartitions)

      initDeltaTable(condensedNewDataDF)

      DeltaTable.forPath(deltaTableDir).toDF
    }

  /** Executes the initial load of the delta table when this algorithm is executed for the first
    * time on the table.
    *
    * @param initialDataDF
    *   DataFrame containing the initial data to load to the delta table
    */
  private def initDeltaTable(initialDataDF: DataFrame): Unit = {
    val dfWriter = {
      if (targetPartitions.nonEmpty) {
        val partitionCols = targetPartitions.map(columnName => col(columnName))
        outputPartitionsNum
          .map(n => initialDataDF.repartition(n, partitionCols: _*))
          .getOrElse(initialDataDF)
          .write
          .format("delta")
          .partitionBy(targetPartitions: _*)
      } else
        outputPartitionsNum
          .map(initialDataDF.repartition)
          .getOrElse(initialDataDF)
          .write
          .format("delta")
    }

    dfWriter.save(deltaTableDir)
  }

  /** Merges the delta table with the new arriving data. Also vacuums old history according to the
    * retention period, if necessary.
    *
    * @param newDataDF
    *   DataFrame containing the new data to merge with the existing data on the delta table
    */
  private def mergeDeltaTable(newDataDF: DataFrame): Unit = {
    val deltaTable = DeltaTable.forPath(deltaTableDir)

    if (isToVacuum) deltaTable.vacuum(vacuumRetentionPeriod)

    deltaTable
      .alias(currentDataAlias)
      .merge(
        newDataDF.alias(newDataAlias),
        generateMatchCondition(businessKey, businessKeyMatchOperator)
      )
      .whenMatched(newDataDF(recordModeColumnName).isin(recordsToDelete: _*))
      .delete()
      .whenMatched()
      .updateAll()
      .whenNotMatched
      .insertAll()
      .execute()
  }

  /** Creation of a condensed set of records to be inserted/updated/deleted. This serves the purpose
    * of taking only the most recent status of a specific record, as the same can can be subjected
    * to several updates (including deletions) in the source which are not yet registered in the
    * delta table, and we want to only store the latest status in the delta table. Steps: 1) Order
    * changes in each record according to the provided business and condensation keys. There is an
    * additional condensation key in cases where, in the init loads only, the record mode column
    * needs to be included in the condensation logic itself (e.g., cases where on the init load
    * there is more than one row per business key); 2) Get only the last change of the records
    *
    * @param newData
    *   DataFrame containing all new delta records
    * @param initLoad
    *   indicates if the condensation is an init load condensation or a delta condensation, as there
    *   is the need to sometimes include the record_mode in the condensation key to properly order
    *   the delta init.
    * @return
    *   DataFrame containing the most recent records to be inserted/updated/deleted
    */
  private def condenseNewData(newData: DataFrame, initLoad: Boolean = false): DataFrame = {
    var partitionWindow = Window.partitionBy(businessKey.map(c => col(c)): _*)
    if (initLoad && initCondensationWithRecordMode)
      partitionWindow = partitionWindow.orderBy(
        condensationKey.map(c => col(c).desc).union(Seq(col(recordModeColumnName).asc)): _*
      )
    else partitionWindow = partitionWindow.orderBy(condensationKey.map(c => col(c).desc): _*)

    val rankedDeltaRecords = newData
      .withColumn("ranking", row_number().over(partitionWindow))
      .filter(recordModesFilterFunction)
    rankedDeltaRecords.filter(rankedDeltaRecords("ranking") === 1).drop("ranking")
  }

  /** Adds temporal partitions (e.g., day, month, year) to an existing DataFrame containing a
    * temporal column
    *
    * @param df
    *   DataFrame to which to add the partitions
    * @return
    */
  def withDatePartitions(df: DataFrame): DataFrame =
    try if (
      targetPartitions.nonEmpty && partitionSourceColumn.nonEmpty &&
      partitionSourceColumnFormat.nonEmpty
    )
      df.transform(
        withDateComponents(partitionSourceColumn, partitionSourceColumnFormat, targetPartitions)
      )
    else df
    catch {
      case e: Throwable =>
        throw new RuntimeException("Cannot add partitioning information for data frames", e)
    }

  /** Generates the condition to match the keys of the current data and new data in the delta table
    *
    * @param businessKey
    *   simple or composite business key that uniquely identifies a record so that new data can be
    *   compared with current data for deciding whether to update or insert a record
    * @param logicalOperator
    *   logical operator in the match condition, in order to compare currentData with NewData
    *   according to the business key. Defaults to AND
    * @return
    *   a string containing the match condition (e.g., current.id1 = new.id1 AND current.id2 =
    *   new.id2)
    */
  private def generateMatchCondition(businessKey: Seq[String], logicalOperator: String): String = {

    /** Ensures that the delta merge match condition always have the null partition spec into
      * consideration, otherwise it may not be correct in rare cases where in the init load there
      * were two initial rows with different record modes for the same row and the condensation
      * logic, despite including the record mode column in the sorting part of the condensation,
      * does not pick the desired one for a very specific organizational business process.
      *
      * This acts mostly as a safe guard for data sources that don't handle empty dates very well in
      * their change logs, so with this we always ensure that we will search for matches in the null
      * partitions (e.g., ((year, 9999), (month,99), (day,99))
      *
      * @param affectedPartitions
      *   affected partitions
      * @param targetPartitions
      *   target partitions
      * @return
      */
    def forceAdditionOfNullPartitionCriteria(
        affectedPartitions: Seq[PartitionCriteria],
        targetPartitions: Seq[String]
    ): Seq[PartitionCriteria] = {
      var partitionCriteria: PartitionCriteria = Seq.empty
      targetPartitions.foreach {
        case partitionName @ "year" =>
          partitionCriteria = partitionCriteria :+
            (partitionName, DateComponentDerivation.DEFAULT_4DIGIT_VALUE.toString)
        case partitionName @ "month" =>
          partitionCriteria = partitionCriteria :+
            (partitionName, DateComponentDerivation.DEFAULT_2DIGIT_VALUE.toString)
        case partitionName @ "day" =>
          partitionCriteria = partitionCriteria :+
            (partitionName, DateComponentDerivation.DEFAULT_2DIGIT_VALUE.toString)
        case partitionName @ "week" =>
          partitionCriteria = partitionCriteria :+
            (partitionName, DateComponentDerivation.DEFAULT_2DIGIT_VALUE.toString)
      }

      affectedPartitions.union(Seq(partitionCriteria)).distinct
    }

    /** Given a collection of column names it builds the adequate delta merge match condition. E.g.:
      * Given Seq("sales_id", "sales_date") it returns currentData.sales_id = newData.sales_id AND
      * currentData.sales_date = newData.sales_date
      *
      * @param columns
      *   list of column names to build the match condition
      * @return
      */
    def generateCondition(columns: Seq[String]): String =
      columns
        .map(key => s"$currentDataAlias.$key = $newDataAlias.$key")
        .mkString(s" $logicalOperator ")

    targetPartitions match {
      case _ :: _ =>
        if (!ignoreAffectedPartitionsMerge) generateCondition(businessKey.union(targetPartitions))
        else
          s"${generateCondition(businessKey)} $logicalOperator (${generateAffectedPartitionsWhere(forceAdditionOfNullPartitionCriteria(affectedPartitions, targetPartitions), s"$currentDataAlias.")})"
      case _ => generateCondition(businessKey)
    }
  }

  /** Generates a string with the where clause used to filter a DataFrame to only select data within
    * the partitions affected in this delta load process.
    *
    * @param affectedPartitions
    *   collection of affected partitions in this delta load process
    * @param prefix
    *   optional prefix to add before the name of the partition column (e.g., currenData.year) to
    *   solve ambiguous column names in the merge operations.
    * @return
    */
  def generateAffectedPartitionsWhere(
      affectedPartitions: Seq[PartitionCriteria],
      prefix: String = ""
  ): String =
    affectedPartitions
      .map(partitionCriteria =>
        s"(%s)".format(
          partitionCriteria
            .map(partition => s"$prefix${partition._1} = ${partition._2}")
            .mkString(" AND ")
        )
      )
      .mkString(" OR ")

  /** Repartitions the delta table
    *
    * @param deltaTable
    *   delta table DataFrame to repartition
    * @param affectedPartitions
    *   sequence of affected partitions considering the new data
    * @param affectedPartitionsFilter
    *   filter function to filter only the affected partitions
    */
  private def repartitionDeltaTable(
      deltaTable: DeltaTable,
      affectedPartitions: Seq[DataFrameUtils.PartitionCriteria],
      affectedPartitionsFilter: FilterFunction
  ): Unit = {

    /** Auxiliary method to repartition delta table independently if the same is partitioned or not
      *
      * @param deltaTableDF
      *   delta table DataFrame
      * @param options
      *   options to repartition the delta table (e.g., dataChange and replaceWhere)
      */
    def repartitionDeltaTableAux(deltaTableDF: DataFrame, options: Map[String, String]): Unit =
      outputPartitionsNum
        .map(deltaTableDF.repartition)
        .getOrElse(deltaTableDF)
        .write
        .options(options)
        .format("delta")
        .mode("overwrite")
        .save(deltaTableDir)

    val (options, df) = {
      val options = mutable.Map("dataChange" -> "false")
      var df = deltaTable.toDF
      if (targetPartitions.nonEmpty) {
        options.put("replaceWhere", generateAffectedPartitionsWhere(affectedPartitions))
        df = df.filter(affectedPartitionsFilter)
      }
      (options, df)
    }

    repartitionDeltaTableAux(df, options.toMap)
  }

  /** Implementation of the update statistics method from the Algorithm trait
    *
    * @param dataFrames
    *   Dataframes to compute statistics
    */
  override protected def updateStatistics(dataFrames: Vector[DataFrame]): Unit =
    if (computeTableStatistics) {
      if (targetPartitions.nonEmpty)
        computeStatisticsForTablePartitions(targetTable, affectedPartitions)
      computeStatisticsForTable(Some(targetTable))
    }
}

object DeltaLakeLoad {

  def apply(spark: SparkSession, dfs: DFSWrapper, configLocation: String): DeltaLakeLoad =
    new DeltaLakeLoad(spark, dfs, configLocation)
}
