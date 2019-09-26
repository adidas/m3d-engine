M3D Engine
=======

**M3D** stands for _Metadata Driven Development_ and is a cloud and platform agnostic framework for the automated creation, management and governance of metadata and data flows from multiple source to multiple target systems. The main features and design goals of M3D are:

*   Cloud and platform agnostic
*   Enforcement global data model including speaking names and business objects
*   Governance by conventions instead of maintaining state and logic
*   Lightweight and easy to use
*   Flexible development of new features
*   Stateless execution with minimal external dependencies
*   Enable self-service
*   Possibility to extend to multiple destination systems (currently AWS EMR)

M3D consists of two components. m3d-engine, which we are providing in this repo, and [m3d-api](https://github.com/adidas/m3d-api) which contains the api as python module.

The architecture of M3D is described in detail [here](https://github.com/adidas/m3d-api).

### Use cases

M3D can be used for:

*  Creation of data lake environments
*  Management and governance of metadata
*  Data flows from multiple sources
*  Data flows to multiple target systems
*  Algorithms as data frame transformations

adidas is not responsible for the usage of this software for different purposes that the ones described in the use cases.

### M3D Engine

**M3D Engine** is a framework written in Scala for distributed execution of ingestion and transformation workloads to and within data lake.

### Algorithms

In M3D terminology an algorithm can be for example:
*   a data transformation from a source on the data lake to a target on the data lake
*   a data load from raw files on the landing layer to the parquet files on the lake layer
*   decompression of compressed data
*   materialization of partitioned data

### M3D Engine Features

M3D Engine supports:

*   Loading structured and semi-structured data in Full mode
*   Loading structured and semi-structured data in Append mode
*   Loading structured and semi-structured data in Delta mode
*   Decompression of compressed data
*   Extraction from parquet file format
*   Extraction from delimiter separated files (CSV,TSV,etc.)
*   Extraction from fixed length string data
*   Partitioned materialization of different types (full, range, query)
*   Usable from jupyter notebooks (using the JavaConsumable trait) 
*   Extensible with new algorithms

### Usage

To execute an algorithm implemented in m3d-engine, it is required to have a Spark cluster running 
that can access a parameters file and the compiled m3d-engine jar artifact.
To execute an Algorithm use can call `spark-submit` with:

```bash
 spark-submit --master yarn \
 --deploy-mode cluster --class com.adidas.analytics.AlgorithmFactory \
 s3://application_bucket/m3d/test/m3d/m3d-api/m3d-engine-assembly.jar \
 FullLoad s3://application_bucket/m3d/test/apps/m3d-engine/fullload/bdp-emr_prod-test.fullload.20190815T134744.json
``` 

#### Input parameters for `m3d-engine-assembly.jar`
*   `appClassName` class name of the algorithm to be executed
*   `appParamFile` location of the parameters file

#### Specification of the parameter file
The parameter file is a `json` file containing algorithm specific configuration.

The parameter file for the full load algorithm for example has the following content:

```json
{
  "current_dir": "s3://lake_bucket/test/source_system/table_name/data/", 
  "backup_dir": "s3://lake_bucket/test/source_system/table_name/data_backup/", 
  "delimiter": "|", 
  "file_format": "dsv", 
  "has_header": false, 
  "partition_column": "date_column_name", 
  "partition_column_format": "yyyyMMdd", 
  "partition_columns": [
      "year", 
      "month"
  ], 
  "source_dir": "s3://landing_bucket/test/source_system/table_name/data/", 
  "target_table": "test_lake.table_name"
}
```

*   `current_dir` location of the currently stored data and where it should be written by the algorithm
*   `backup_dir` backup location of the data before the existing data is overwritten
*   `source_dir` location of the source data to be ingested
*   `file_format` format of the source data, e.g. `dsv` or `parquet`
*   `delimiter` delimiter used in the case of `dsv` format
*   `has_header` flag defining whether the input files have a header
*   `partition_column` column that contains the partitioning information
*   `partition_column_format` format of the partitioning column in the case of of time/date columns
*   `partition_columns`  partitioning columns
*   `target_table` target table where the data will be available for querying after loading

### License and Software Information
 
© adidas AG
 
adidas AG publishes this software and accompanied documentation (if any) subject to the terms of the Apache 2.0 license with the aim of helping the community with our tools and libraries which we think can be also useful for other people. You will find a copy of the Apache 2.0 license in the root folder of this package. All rights not explicitly granted to you under the Apache 2.0 license remain the sole and exclusive property of adidas AG.
 
NOTICE: The software has been designed solely for the purpose of automated creation, management and governance of metadata and data flows. The software is NOT designed, tested or verified for productive use whatsoever, nor or for any use related to high risk environments, such as health care, highly or fully autonomous driving, power plants, or other critical infrastructures or services.
 
If you want to contact adidas regarding the software, you can mail us at _software.engineering@adidas.com_.
 
For further information open the [adidas terms and conditions](https://github.com/adidas/adidas-contribution-guidelines/wiki/Terms-and-conditions) page.

#### License

[Apache 2.0](LICENSE)