package com.adidas.analytics.algo.loads

import java.util.regex.Pattern
import com.adidas.analytics.algo.core.{Algorithm, TableStatistics}
import com.adidas.analytics.algo.loads.AppendLoad._
import com.adidas.analytics.algo.shared.DateComponentDerivation
import com.adidas.analytics.config.loads.AppendLoadConfiguration
import com.adidas.analytics.util.DFSWrapper._
import com.adidas.analytics.util.DataFormat.{DSVFormat, JSONFormat, ParquetFormat}
import com.adidas.analytics.util.DataFrameUtils._
import com.adidas.analytics.util._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable

/** Performs append load of new records to an existing table.
  */
final class AppendLoad protected (
    val spark: SparkSession,
    val dfs: DFSWrapper,
    val configLocation: String
) extends Algorithm
    with AppendLoadConfiguration
    with TableStatistics
    with DateComponentDerivation {

  override protected def read(): Vector[DataFrame] =
    if (regexFilename.nonEmpty || partitionSourceColumn.nonEmpty)
      readInputData(targetSchema, spark, dfs)
    else
      Vector(
        readInputFiles(
          Seq(inputDir),
          fileFormat,
          if (!verifySchema) Some(targetSchema) else None,
          spark.read.options(sparkReaderOptions)
        )
      )

  override protected def transform(dataFrames: Vector[DataFrame]): Vector[DataFrame] =
    if (partitionSourceColumn.nonEmpty)
      dataFrames.map(df =>
        df.transform(
          withDateComponents(
            partitionSourceColumn.get,
            partitionSourceColumnFormat.getOrElse("yyyy-MM-dd"),
            targetPartitions
          )
        )
      )
    else if (regexFilename.nonEmpty)
      dataFrames.map(df =>
        df.transform(addTargetPartitions(targetPartitions zip regexFilename.get, targetSchema))
      )
    else dataFrames

  override protected def write(dataFrames: Vector[DataFrame]): Vector[DataFrame] = {
    writeHeaders(dataFrames, targetPartitions, headerDir, dfs)
    super.write(dataFrames)
  }

  override protected def updateStatistics(dataFrames: Vector[DataFrame]): Unit =
    if (computeTableStatistics && dataType == STRUCTURED && targetTable.isDefined) {
      if (targetPartitions.nonEmpty)
        dataFrames
          .foreach(df => computeStatisticsForTablePartitions(df, targetTable.get, targetPartitions))
      computeStatisticsForTable(targetTable)
    }

  private def readInputData(
      targetSchema: StructType,
      spark: SparkSession,
      dfs: DFSWrapper
  ): Vector[DataFrame] = {
    val inputDirPath = new Path(inputDir)
    val headerDirPath = new Path(headerDir)

    val fs = dfs.getFileSystem(inputDirPath)
    val sources = listSources(inputDirPath, headerDirPath, fs, targetSchema)
    readSources(sources, spark)
  }

  private def listSources(
      inputDirPath: Path,
      headerDirPath: Path,
      fs: FileSystem,
      targetSchema: StructType
  ): Seq[Source] = {
    logger.info(s"Looking for input files in $inputDirPath")
    val targetSchemaWithoutTargetPartitions =
      getSchemaWithoutTargetPartitions(targetSchema, targetPartitions.toSet)
    if (partitionSourceColumn.nonEmpty)
      fs.ls(inputDirPath, recursive = true)
        .map(sourcePath => Source(targetSchemaWithoutTargetPartitions, sourcePath.toString))
    else {
      val groupedHeaderPathAndSourcePaths =
        fs.ls(inputDirPath, recursive = true).groupBy { inputPath =>
          buildHeaderFilePath(
            targetPartitions zip regexFilename.get,
            targetSchema,
            extractPathWithoutServerAndProtocol(inputPath.toString),
            headerDirPath
          )
        }

      def getMapSchemaStructToPath: immutable.Iterable[Source] = {
        val mapSchemaStructToPath = groupedHeaderPathAndSourcePaths.toSeq
          .map {
            case (headerPath, sourcePaths) =>
              getSchemaFromHeaderOrSource(fs, headerPath, sourcePaths)
          }
          .groupBy(_._1)
          .map { case (k, v) => (k, v.flatMap(_._2)) }

        val filteredMapSchemaStructToPath = mapSchemaStructToPath.filter(schemaFromInputData =>
          matchingSchemas_?(schemaFromInputData._1, targetSchema, schemaFromInputData._2)
        )

        if (mapSchemaStructToPath.size != filteredMapSchemaStructToPath.size)
          throw new RuntimeException(
            "Schema does not match the input data for some of the input folders."
          )

        mapSchemaStructToPath.flatMap {
          case (_, sourcePaths) =>
            sourcePaths.map(sourcePath => Source(targetSchema, sourcePath.toString))
        }
      }

      val schemaAndSourcePath =
        if (!verifySchema) groupedHeaderPathAndSourcePaths.flatMap {
          case (headerPath, sourcePaths) =>
            val schema =
              if (fs.exists(headerPath)) loadHeader(headerPath, fs)
              else targetSchemaWithoutTargetPartitions
            sourcePaths.map(sourcePath => Source(schema, sourcePath.toString))
        }
        else getMapSchemaStructToPath
      schemaAndSourcePath.toSeq
    }
  }

  private def getSchemaFromHeaderOrSource(
      fs: FileSystem,
      headerPath: Path,
      sourcePaths: Seq[Path]
  ): (StructType, Seq[Path]) = {
    val schema =
      if (fs.exists(headerPath)) loadHeader(headerPath, fs) else inferSchemaFromSource(sourcePaths)
    (schema, sourcePaths)
  }

  private def inferSchemaFromSource(sourcePaths: Seq[Path]): StructType = {
    val reader = spark.read.options(sparkReaderOptions)
    val dataFormat = fileFormat match {
      case "dsv"         => DSVFormat()
      case "parquet"     => ParquetFormat()
      case "json"        => JSONFormat()
      case anotherFormat => throw new RuntimeException(s"Unknown file format: $anotherFormat")
    }
    dataFormat.read(reader, sourcePaths.map(_.toString): _*).schema
  }

  private def matchingSchemas_?(
      schemaFromInputData: StructType,
      targetSchema: StructType,
      paths: Seq[Path]
  ): Boolean = {
    val inputColumnsVector = schemaFromInputData.names.toVector
    val targetColumnsVector = targetSchema.names.toVector
    val diff = inputColumnsVector.diff(targetColumnsVector)
    if (diff.nonEmpty)
      logger.error(s"Inferred schema does not match the target schema for ${paths.toString}")
    diff.isEmpty
  }

  private def readSources(sources: Seq[Source], spark: SparkSession): Vector[DataFrame] =
    groupSourcesBySchema(sources).map {
      case (schema, inputPaths) =>
        readInputFiles(inputPaths, fileFormat, Some(schema), spark.read.options(sparkReaderOptions))
    }.toVector

  private def readInputFiles(
      inputPaths: Seq[String],
      fileFormat: String,
      schema: Option[StructType],
      reader: DataFrameReader
  ): DataFrame =
    fileFormat match {
      case "dsv"         => DSVFormat(schema, multiLine = isMultiline).read(reader, inputPaths: _*)
      case "parquet"     => ParquetFormat(schema).read(reader, inputPaths: _*)
      case "json"        => JSONFormat(schema, multiLine = isMultiline).read(reader, inputPaths: _*)
      case anotherFormat => throw new RuntimeException(s"Unknown file format: $anotherFormat")
    }
}

object AppendLoad {

  private val logger: Logger = LoggerFactory.getLogger(getClass)
  private val headerFileName: String = "header.json"

  def apply(spark: SparkSession, dfs: DFSWrapper, configLocation: String): AppendLoad =
    new AppendLoad(spark, dfs, configLocation)

  private def extractPathWithoutServerAndProtocol(path: String): String =
    path.replaceFirst("\\w+\\d*://.+?/", "")

  private def getSchemaWithoutTargetPartitions(
      targetSchema: StructType,
      targetPartitions: Set[String]
  ): StructType =
    StructType(targetSchema.fields.filterNot(field => targetPartitions.contains(field.name)))

  private def groupSourcesBySchema(sources: Seq[Source]): Map[StructType, Seq[String]] =
    sources.groupBy(_.schema).mapValues(sources => sources.map(_.inputFileLocation))

  private def addTargetPartitions(
      columnNameToRegexPairs: Seq[(String, String)],
      schema: StructType
  )(inputDf: DataFrame): DataFrame = {
    def getInputFileName: Column =
      udf((path: String) => extractPathWithoutServerAndProtocol(path)).apply(input_file_name)

    val tempInputFileNameColumn = col("temp_input_file_name")
    val columnNameToTypeMapping = schema.fields.map(field => field.name -> field.dataType).toMap

    columnNameToRegexPairs
      .foldLeft(inputDf.withColumn(tempInputFileNameColumn.toString, getInputFileName)) {
        case (df, (columnName, regex)) =>
          val targetColumnType = columnNameToTypeMapping(columnName)
          df.withColumn(
            columnName,
            regexp_extract(tempInputFileNameColumn, regex, 1).cast(targetColumnType)
          )
      }
      .drop(tempInputFileNameColumn)
  }

  private def buildHeaderFilePath(
      columnNameToRegexPairs: Seq[(String, String)],
      schema: StructType,
      inputFileName: String,
      headerDirPath: Path
  ): Path = {
    val columnNameToTypeMapping = schema.fields.map(field => field.name -> field.dataType).toMap
    val subdirectories = columnNameToRegexPairs.map {
      case (columnName, regex) =>
        implicit val dataType: DataType = columnNameToTypeMapping(columnName)
        extractPartitionColumnValue(inputFileName, regex) match {
          case Some(columnValue) => s"$columnName=$columnValue"
          case None =>
            throw new RuntimeException(
              s"Unable to extract value for $columnName with '$regex' from $inputFileName"
            )
        }
    }
    new Path(headerDirPath.join(subdirectories), headerFileName)
  }

  private def loadHeader(headerPath: Path, fs: FileSystem): StructType =
    DataType.fromJson(fs.readFile(headerPath)).asInstanceOf[StructType]

  protected def writeHeaders(
      dataFrames: Seq[DataFrame],
      targetPartitions: Seq[String],
      headerDir: String,
      dfs: DFSWrapper
  ): Unit = {
    logger.info(s"Writing header files to $headerDir")
    val headerDirPath = new Path(headerDir)
    val fs = dfs.getFileSystem(headerDirPath)
    dataFrames.foreach { df =>
      val schemaJson =
        getSchemaWithoutTargetPartitions(df.schema, targetPartitions.toSet).prettyJson
      df.collectPartitions(targetPartitions).foreach { partitionCriteria =>
        val subdirectories = DataFrameUtils.mapPartitionsToDirectories(partitionCriteria)
        val headerPath = new Path(headerDirPath.join(subdirectories), headerFileName)
        if (!fs.exists(headerPath)) {
          logger.info(s"Writing header $headerPath")
          fs.writeFile(headerPath, schemaJson)
        }
      }
    }
  }

  private def extractPartitionColumnValue(fileName: String, regex: String)(implicit
      dataType: DataType
  ): Option[String] = {
    val matcher = Pattern.compile(regex).matcher(fileName)
    Option(matcher)
      .filter(_.find)
      .map(_.group(1)) //modifications to regexes demand taking group 1 instead of group 0
      .map(restoreFromTypedValue)
  }

  private def restoreFromTypedValue(
      stringColumnValue: String
  )(implicit dataType: DataType): String = {
    val columnValue = dataType match {
      case ByteType | ShortType | IntegerType | LongType => stringColumnValue.toLong
      case BooleanType                                   => stringColumnValue.toBoolean
      case StringType                                    => stringColumnValue
    }
    columnValue.toString
  }

  protected case class Source(schema: StructType, inputFileLocation: String)

}
