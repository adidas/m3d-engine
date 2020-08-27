package com.adidas.analytics.util

import com.adidas.analytics.algo.shared.DateComponentDerivation
import com.adidas.analytics.util.DFSWrapper._
import com.adidas.analytics.util.CatalogTableManager._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.util.{FailFastMode, PermissiveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

class CatalogTableManager(table: String, sparkSession: SparkSession) {
  private val KeyColumn = col("col_name")
  private val ValueColumn = col("data_type")
  private val LocationKey = "LOCATION"

  private lazy val attributes: Map[String, String] = sparkSession
    .sql(s"describe formatted $table")
    .select(KeyColumn, ValueColumn)
    .withColumn(KeyColumn.toString, upper(KeyColumn))
    .collect()
    .map(row => (row.getAs[String](KeyColumn.toString), row.getAs[String](ValueColumn.toString)))
    .toMap

  /** Generic method to read attribute values from hive table description output
    *
    * @param key
    *   Key to be read (First column)
    * @return
    *   Value (Second Column)
    */
  def getValueForKey(key: String): String = attributes(key.toUpperCase)

  /** Read table location
    *
    * @return
    *   table location
    */
  def getTableLocation: String = getValueForKey(LocationKey)

  /** Set table location
    *
    * @param location
    *   location of the table
    * @return
    *   DataFrame
    */
  def setTableLocation(location: String): Unit =
    sparkSession.sql(s"ALTER TABLE $table SET $LocationKey '$location'")

  /** Set location of all table partitions
    *
    * @param location
    *   location of the table
    */
  def setTablePartitionsLocation(location: String): Unit = {
    val partitionSpecs = sparkSession.sql(s"SHOW PARTITIONS $table").collect()
    partitionSpecs.foreach { row =>
      val originalSpec = row.getAs[String]("partition")
      val formattedSpec = getFormattedPartitionSpec(row)
      sparkSession.sql(
        s"ALTER TABLE $table PARTITION ($formattedSpec) SET $LocationKey '$location/$originalSpec'"
      )
    }
  }

  /** Drop all table partitions
    */
  def dropTablePartitions(): Unit = {
    val partitionSpecs = sparkSession.sql(s"SHOW PARTITIONS $table").collect()
    partitionSpecs.foreach { row =>
      val formattedSpec = getFormattedPartitionSpec(row)
      sparkSession.sql(s"ALTER TABLE $table DROP IF EXISTS PARTITION ($formattedSpec)")
    }
  }

  /** Recreate the table in a new location by creating a new table like the old one in terms of
    * schema. It uses a temporary table to transition from the old table definition to the new one,
    * because the old table cannot be dropped until the new one is created.
    *
    * @param location
    *   location for the target table
    * @param targetPartitions
    *   target table partitions
    */
  def recreateTable(location: String, targetPartitions: Seq[String]): Unit = {
    logger.info(s"Recreating table $table in location: $location")

    sparkSession.sql(createExternalTableStatement(table, s"${table}_tmp", location))
    sparkSession.sql(s"DROP TABLE IF EXISTS $table")

    sparkSession.sql(createExternalTableStatement(s"${table}_tmp", table, location))
    sparkSession.sql(s"DROP TABLE IF EXISTS ${table}_tmp")

    if (targetPartitions.nonEmpty) sparkSession.catalog.recoverPartitions(table)

    logger.info(s"Finished recreating table $table in location: $location")
  }

  /** A method to get the target schema by first checking if the data folder exists. If not, data
    * folder is created, avoiding getSchema to fail for certain Hive/Spark metastores. Target schema
    * is given accordingly to the respective reader mode.
    *
    * @param dfs
    *   DFSWrapper to create directory if the same does not exist
    * @param targetPartitions
    *   List with the target partitions
    * @param isDropDerivedColumns
    *   Boolean to remove or maintain target partitions in table initial schema
    * @param addCorruptRecord
    *   Boolean to add corrupt_record column when permissive mode is used
    * @param readerMode
    *   Failfast, Permissive or DropMalformed modes are available
    * @return
    *   Target table schema
    */
  def getSchemaSafely(
      dfs: DFSWrapper,
      targetPartitions: Seq[String] = Seq.empty[String],
      isDropDerivedColumns: Boolean = false,
      addCorruptRecord: Boolean = false,
      readerMode: Option[String] = None
  ): StructType = {

    def dropDateDerivedColumns(schema: StructType): StructType = {
      var schemaStruct = new StructType()
      schema.foreach { column =>
        if (!DateComponentDerivation.ALLOWED_DERIVATIONS.contains(column.name))
          schemaStruct = schemaStruct.add(column)
      }
      schemaStruct
    }

    def addCorruptRecordColumn(schema: StructType): StructType =
      schema.add(StructField("_corrupt_record", StringType, nullable = true))

    try {
      val tableLocation = new Path(getTableLocation)
      dfs.getFileSystem(tableLocation).createDirIfNotExists(tableLocation)
      logger.info(s"Retrieved table location: $tableLocation")

      val schema = sparkSession.table(table).schema
      readerMode match {
        case Some(FailFastMode.name) if isDropDerivedColumns => dropDateDerivedColumns(schema)
        case Some(PermissiveMode.name) =>
          if (isDropDerivedColumns)
            if (addCorruptRecord) addCorruptRecordColumn(dropDateDerivedColumns(schema))
            else dropDateDerivedColumns(schema)
          else if (addCorruptRecord) addCorruptRecordColumn(schema)
          else schema
        case _ => schema
      }
    } catch {
      case e: Throwable =>
        throw new RuntimeException(s"Unable to return or create table location: ", e)
    }
  }

  private def createExternalTableStatement(
      sourceTable: String,
      destinationTable: String,
      location: String
  ): String = s"CREATE TABLE IF NOT EXISTS $destinationTable LIKE $sourceTable LOCATION '$location'"

  private def getFormattedPartitionSpec(row: Row): String =
    row
      .getAs[String]("partition")
      .split('/')
      .map { p =>
        val pSplitted = p.split('=')
        "%s='%s'".format(pSplitted(0), pSplitted(1))
      }
      .mkString(",")
}

object CatalogTableManager {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def apply(tableName: String, sparkSession: SparkSession): CatalogTableManager =
    new CatalogTableManager(tableName, sparkSession)
}
