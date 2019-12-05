package com.adidas.analytics.algo

import java.util.regex.Pattern

import com.adidas.analytics.algo.AppendLoad.{logger, _}
import com.adidas.analytics.algo.core.Algorithm
import com.adidas.analytics.algo.core.Algorithm.ComputeTableStatisticsOperation
import com.adidas.analytics.config.AppendLoadConfiguration
import com.adidas.analytics.util.DFSWrapper._
import com.adidas.analytics.util.DataFormat.{DSVFormat, JSONFormat, ParquetFormat}
import com.adidas.analytics.util.DataFrameUtils._
import com.adidas.analytics.util._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, _}
import org.slf4j.{Logger, LoggerFactory}

/**
  * Performs append load of new records to an existing table.
  */
final class AppendLoad protected(val spark: SparkSession, val dfs: DFSWrapper, val configLocation: String)
  extends Algorithm with AppendLoadConfiguration with ComputeTableStatisticsOperation{

  override protected def read(): Vector[DataFrame] = {
    readInputData(targetSchema, spark, dfs)
  }

  override protected def transform(dataFrames: Vector[DataFrame]): Vector[DataFrame] = {
    dataFrames.map(df => df.transform(addtargetPartitions(columnToRegexPairs, targetSchema)))
  }

  override protected def write(dataFrames: Vector[DataFrame]): Unit = {
    writeHeaders(dataFrames, targetPartitions, headerDir, dfs)
    super.write(dataFrames)
    if (computeTableStatistics && dataType == STRUCTURED)
      computeStatisticsForTable(targetTable)
  }

  private def readInputData(targetSchema: StructType, spark: SparkSession, dfs: DFSWrapper): Vector[DataFrame] = {
    val inputDirPath = new Path(inputDir)
    val headerDirPath = new Path(headerDir)

    val fs = dfs.getFileSystem(inputDirPath)
    val sources = listSources(inputDirPath, headerDirPath, fs, targetSchema)
    readSources(sources, fs, spark)
  }

  private def listSources(inputDirPath: Path, headerDirPath: Path, fs: FileSystem, targetSchema: StructType): Seq[Source] = {
    val targetSchemaWithouttargetPartitions = getSchemaWithouttargetPartitions(targetSchema, targetPartitions.toSet)

    logger.info(s"Looking for input files in $inputDirPath")
    val groupedHeaderPathAndSourcePaths = fs.ls(inputDirPath, recursive = true).groupBy { inputPath =>
      buildHeaderFilePath(columnToRegexPairs, targetSchema, extractPathWithoutServerAndProtocol(inputPath.toString), headerDirPath)
    }

    def getMapSchemaStructToPath = {
      val mapSchemaStructToPath = groupedHeaderPathAndSourcePaths.toSeq.map { case (headerPath, sourcePaths) =>
        getSchemaFromHeaderOrSource(fs, headerPath, sourcePaths, targetSchemaWithouttargetPartitions)
      }.groupBy(_._1).map { case (k, v) => (k, v.flatMap(_._2)) }

      val filteredMapSchemaStructToPath = mapSchemaStructToPath.filter(schemaFromInputData => matchingSchemas_?(schemaFromInputData._1, targetSchema, schemaFromInputData._2))

      if (mapSchemaStructToPath.size != filteredMapSchemaStructToPath.size)
        throw new RuntimeException("Schema does not match the input data for some of the input folders.")

      mapSchemaStructToPath.flatMap { case (schema, sourcePaths) =>
        sourcePaths.map { sourcePath =>
          Source(targetSchema, sourcePath.toString)
        }
      }
    }

    val schemaAndSourcePath = if(!verifySchema) {
      groupedHeaderPathAndSourcePaths.flatMap { case (headerPath, sourcePaths) =>
        val schema = if (fs.exists(headerPath)) loadHeader(headerPath, fs) else targetSchemaWithouttargetPartitions
        sourcePaths.map { sourcePath =>
          Source(schema, sourcePath.toString)
        }
      }
    } else {
      getMapSchemaStructToPath
    }
    schemaAndSourcePath.toSeq
  }

  private def getSchemaFromHeaderOrSource(fs: FileSystem, headerPath: Path, sourcePaths: Seq[Path], targetSchemaWithouttargetPartitions: StructType): (StructType, Seq[Path]) ={
    val schema = if (fs.exists(headerPath)){
      loadHeader(headerPath, fs) }
    else {
      inferSchemaFromSource(sourcePaths)
    }
    (schema, sourcePaths)
  }

  private def inferSchemaFromSource(sourcePaths: Seq[Path]): StructType = {
    val reader = spark.read.options(sparkReaderOptions)
    val dataFormat = fileFormat match {
      case "dsv" => DSVFormat()
      case "parquet" => ParquetFormat()
      case "json" => JSONFormat()
      case anotherFormat => throw new RuntimeException(s"Unknown file format: $anotherFormat")
    }
    dataFormat.read(reader, sourcePaths.map(_.toString): _*).schema
  }

  private def matchingSchemas_?(schemaFromInputData: StructType, targetSchema: StructType, paths: Seq[Path]): Boolean = {
    val inputColumnsVector =  schemaFromInputData.names.toVector
    val targetColumnsVector =  targetSchema.names.toVector
    val diff = inputColumnsVector.diff(targetColumnsVector)
    if(diff.nonEmpty)
      logger.error(s"Inferred schema does not match the target schema for ${paths.toString}")
    diff.isEmpty
  }

  private def readSources(sources: Seq[Source], fs: FileSystem, spark: SparkSession): Vector[DataFrame] = {
    groupSourcesBySchema(sources).map {
      case (schema, inputPaths) => readInputFiles(inputPaths, fileFormat, schema, spark.read.options(sparkReaderOptions))
    }.toVector
  }

  private def readInputFiles(inputPaths: Seq[String], fileFormat: String, schema: StructType, reader: DataFrameReader): DataFrame = {
    fileFormat match {
      case "dsv" => DSVFormat(Some(schema)).read(reader, inputPaths: _*)
      case "parquet" => ParquetFormat(Some(schema)).read(reader, inputPaths: _*)
      case "json" => JSONFormat(Some(schema)).read(reader, inputPaths: _*)
      case anotherFormat => throw new RuntimeException(s"Unknown file format: $anotherFormat")
    }
  }
}


object AppendLoad {

  private val logger: Logger = LoggerFactory.getLogger(getClass)
  private val headerFileName: String = "header.json"

  def apply(spark: SparkSession, dfs: DFSWrapper, configLocation: String): AppendLoad = {
    new AppendLoad(spark, dfs, configLocation)
  }

  private def extractPathWithoutServerAndProtocol(path: String): String = {
    path.replaceFirst("\\w+\\d*://.+?/", "")
  }

  private def getSchemaWithouttargetPartitions(targetSchema: StructType, targetPartitions: Set[String]): StructType = {
    StructType(targetSchema.fields.filterNot(field => targetPartitions.contains(field.name)))
  }

  private def groupSourcesBySchema(sources: Seq[Source]): Map[StructType, Seq[String]] = {
    sources.groupBy(_.schema).mapValues { sources =>
      sources.map(_.inputFileLocation)
    }
  }

  private def addtargetPartitions(columnNameToRegexPairs: Seq[(String, String)], schema: StructType)(inputDf: DataFrame): DataFrame = {
    def getInputFileName: Column = {
      udf((path: String) => extractPathWithoutServerAndProtocol(path)).apply(input_file_name)
    }

    val tempInputFileNameColumn = col("temp_input_file_name")
    val columnNameToTypeMapping = schema.fields.map(field => field.name -> field.dataType).toMap

    columnNameToRegexPairs.foldLeft(inputDf.withColumn(tempInputFileNameColumn.toString, getInputFileName)) {
      case (df, (columnName, regex)) =>
        val targetColumnType = columnNameToTypeMapping(columnName)
        df.withColumn(columnName, regexp_extract(tempInputFileNameColumn, regex, 1).cast(targetColumnType))
    }.drop(tempInputFileNameColumn)
  }

  private def buildHeaderFilePath(columnNameToRegexPairs: Seq[(String, String)], schema: StructType, inputFileName: String, headerDirPath: Path): Path = {
    val columnNameToTypeMapping = schema.fields.map(field => field.name -> field.dataType).toMap
    val subdirectories = columnNameToRegexPairs.map {
      case (columnName, regex) =>
        implicit val dataType: DataType = columnNameToTypeMapping(columnName)
        extractPartitionColumnValue(inputFileName, regex) match {
          case Some(columnValue) => s"$columnName=$columnValue"
          case None => throw new RuntimeException(s"Unable to extract value for $columnName with '$regex' from $inputFileName")
        }
    }
    new Path(headerDirPath.join(subdirectories), headerFileName)
  }

  private def loadHeader(headerPath: Path, fs: FileSystem): StructType = {
    DataType.fromJson(fs.readFile(headerPath)).asInstanceOf[StructType]
  }

  protected def writeHeaders(dataFrames: Seq[DataFrame], targetPartitions: Seq[String], headerDir: String, dfs: DFSWrapper): Unit = {
    logger.info(s"Writing header files to $headerDir")
    val headerDirPath = new Path(headerDir)
    val fs = dfs.getFileSystem(headerDirPath)
    dataFrames.foreach { df =>
      val schemaJson = getSchemaWithouttargetPartitions(df.schema, targetPartitions.toSet).prettyJson
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

  private def extractPartitionColumnValue(fileName: String, regex: String)(implicit dataType: DataType): Option[String] = {
    val matcher = Pattern.compile(regex).matcher(fileName)
    Option(matcher)
      .filter(_.find)
      .map(_.group(1)) //modifications to regexes demand taking group 1 instead of group 0
      .map(restoreFromTypedValue)
  }

  private def restoreFromTypedValue(stringColumnValue: String)(implicit dataType: DataType): String = {
    val columnValue = dataType match {
      case ByteType | ShortType | IntegerType | LongType => stringColumnValue.toLong
      case BooleanType => stringColumnValue.toBoolean
      case StringType => stringColumnValue
    }
    columnValue.toString
  }

  protected case class Source(schema: StructType, inputFileLocation: String)
}
