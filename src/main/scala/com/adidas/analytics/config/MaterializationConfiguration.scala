package com.adidas.analytics.config

import com.adidas.analytics.algo.core.Algorithm.{
  ReadOperation,
  SafeWriteOperation,
  UpdateStatisticsOperation
}
import com.adidas.analytics.config.shared.MetadataUpdateStrategy
import com.adidas.analytics.util.DataFormat.ParquetFormat
import com.adidas.analytics.util.DataFrameUtils.PartitionCriteria
import com.adidas.analytics.util.{
  ConfigReader,
  HadoopLoadHelper,
  CatalogTableManager,
  InputReader,
  LoadMode,
  OutputWriter
}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.joda.time._
import org.joda.time.format.DateTimeFormat

trait MaterializationConfiguration
    extends ReadOperation
    with SafeWriteOperation
    with UpdateStatisticsOperation
    with MetadataUpdateStrategy {

  protected def configReader: ConfigReader

  protected def spark: SparkSession

  protected def loadMode: LoadMode

  protected def partitionsCriteria: Seq[PartitionCriteria]

  protected val sourceTable: String = configReader.getAs[String]("source_table")
  protected val targetTable: String = configReader.getAs[String]("target_table")

  protected val targetSchema: StructType =
    CatalogTableManager(targetTable, spark).getSchemaSafely(dfs)

  protected val targetPartitions: Seq[String] =
    configReader.getAsSeq[String]("target_partitions").toList

  protected val toCache: Boolean = configReader.getAsOption[Boolean]("to_cache").getOrElse(true)

  override protected val readers: Vector[InputReader.TableReader] =
    Vector(InputReader.newTableReader(table = sourceTable))

  override protected val writer: OutputWriter.AtomicWriter = OutputWriter.newTableLocationWriter(
    table = targetTable,
    format = ParquetFormat(Some(targetSchema)),
    targetPartitions = targetPartitions,
    loadMode = loadMode,
    metadataConfiguration = getMetaDataUpdateStrategy(targetTable, targetPartitions)
  )

  override protected def outputFilesNum: Option[Int] =
    configReader.getAsOption[Int]("number_output_partitions")
}

object MaterializationConfiguration {

  private val ConditionPattern = "(.+?)=(.+)".r

  private val FormatYearMonthDay = "yyyy-MM-dd"
  private val FormatYearMonth = "yyyy-MM"
  private val FormatYearWeek = "yyyy-ww"

  private val Year = "year"
  private val Month = "month"
  private val Week = "week"
  private val Day = "day"

  trait FullMaterializationConfiguration extends MaterializationConfiguration {

    override protected val partitionsCriteria: Seq[PartitionCriteria] = Seq.empty

    protected val numVersionsToRetain: Int = configReader.getAs[Int]("num_versions_to_retain")

    protected val baseDataDir: String = configReader.getAs[String]("base_data_dir")

    protected val currentTableLocation: Path =
      new Path(CatalogTableManager(targetTable, spark).getTableLocation)

    protected val sortingIgnoreFolderNames: Seq[String] =
      configReader
        .getAsOption[Seq[String]]("sorting_ignore_folder_names")
        .getOrElse(Seq("_$folder$", "=", ".parquet", "_SUCCESS", "_tmp_"))

    protected val tableDataDir: Path = {
      if (currentTableLocation.getName == baseDataDir.replace("/", ""))
        /* currentTableLocation is baseDataDir (e.g., data/), so tableDataDir will be the same */
        currentTableLocation
      else
        /* currentTableLocation is e.g., data/20200612_101214_UTC, so we need to get the parent
         * folder */
        currentTableLocation.getParent
    }

    protected val nextTableLocation: Path =
      HadoopLoadHelper.buildUTCTimestampedTablePath(tableDataDir)
    protected val nextTableLocationPrefix: String = nextTableLocation.getName

    override protected val writer: OutputWriter.AtomicWriter = OutputWriter.newFileSystemWriter(
      location = nextTableLocation.toString,
      format = ParquetFormat(Some(targetSchema)),
      targetPartitions = targetPartitions,
      loadMode = loadMode
    )
  }

  trait QueryMaterializationConfiguration extends MaterializationConfiguration {

    override protected val partitionsCriteria: Seq[PartitionCriteria] = {
      val conditions = configReader.getAsSeq[String]("select_conditions").map {
        case ConditionPattern(columnName, columnValue) => (columnName.trim, columnValue.trim)
        case condition                                 => throw new IllegalArgumentException(s"Wrong select condition: $condition")
      }

      if (conditions.isEmpty)
        throw new RuntimeException(s"Unable to run materialization by query: conditions are empty")

      Seq(conditions)
    }
  }

  trait RangeMaterializationConfiguration extends MaterializationConfiguration {

    private val fromDateString = configReader.getAs[String]("date_from")
    private val toDateString = configReader.getAs[String]("date_to")

    override protected val partitionsCriteria: Seq[PartitionCriteria] = targetPartitions match {
      case Year :: Month :: Day :: Nil =>
        getDatesRange(FormatYearMonthDay, Days.ONE).map { date =>
          Seq(
            Year -> date.getYear.toString,
            Month -> date.getMonthOfYear.toString,
            Day -> date.getDayOfMonth.toString
          )
        }.toSeq
      case Year :: Month :: Nil =>
        getDatesRange(FormatYearMonth, Months.ONE).map { date =>
          Seq(Year -> date.getYear.toString, Month -> date.getMonthOfYear.toString)
        }.toSeq
      case Year :: Week :: Nil =>
        getDatesRange(FormatYearWeek, Weeks.ONE).map { date =>
          Seq(Year -> date.getYear.toString, Week -> date.getWeekOfWeekyear.toString)
        }.toSeq
      case _ =>
        throw new RuntimeException(
          s"Unable to run materialization by date range: unsupported partitioning schema: $targetPartitions"
        )
    }

    private def getDatesRange(pattern: String, period: ReadablePeriod): Iterator[LocalDate] = {
      val dateFormatter = DateTimeFormat.forPattern(pattern)
      val startDate = LocalDate.parse(fromDateString, dateFormatter)
      val endDate = LocalDate.parse(toDateString, dateFormatter)
      if (startDate.isAfter(endDate))
        throw new RuntimeException(
          "Unable to run materialization by date range: date_start is after date_end"
        )
      Iterator.iterate(startDate)(_.plus(period)).takeWhile(!_.isAfter(endDate))
    }
  }

}
