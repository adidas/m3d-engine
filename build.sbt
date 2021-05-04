import sbt.ExclusionRule

name := "m3d-engine"
version := "6.0.0"

scalaVersion := "2.12.13"
addCompilerPlugin("org.scalameta" % "semanticdb-scalac" % "4.4.11" cross CrossVersion.full)
scalacOptions += "-Yrangepos"
semanticdbEnabled := true
scalacOptions += "-Ywarn-unused-import"

val sparkVersion = "3.0.1"
val hadoopVersion = "3.2.1"

conflictManager := sbt.ConflictManager.latestRevision

Compile / mainClass := Some("com.adidas.analytics.AlgorithmFactory")

/* =====================
 * Dependencies
 * ===================== */

libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion % Provided withExclusions Vector(
  ExclusionRule("org.apache.hadoop", "hadoop-common"),
  ExclusionRule("org.apache.hadoop", "hadoop-hdfs"),
  ExclusionRule("com.google.guava", "guava")
)

libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion % Provided withExclusions Vector(
  ExclusionRule("org.apache.hadoop", "hadoop-hdfs")
)

libraryDependencies += "org.apache.hadoop" % "hadoop-common" % hadoopVersion % Provided withExclusions Vector(
  ExclusionRule("io.netty", "netty-all")
)

libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % Provided
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs-client" % hadoopVersion % Provided
libraryDependencies += "org.apache.hadoop" % "hadoop-distcp" % hadoopVersion % Provided

libraryDependencies += "joda-time" % "joda-time" % "2.10.10" % Provided
libraryDependencies += "org.joda" % "joda-convert" % "2.2.1"

libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.30"

libraryDependencies += "io.delta" %% "delta-core" % "0.8.0"

/* =====================
 * Dependencies for test
 * ===================== */

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.7" % Test

libraryDependencies +=
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % Test classifier "tests" withExclusions Vector(
    ExclusionRule("io.netty", "netty-all")
  )
libraryDependencies +=
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % Test classifier "tests" withExclusions Vector(
    ExclusionRule("io.netty", "netty-all")
  )

Test / fork := true

// disable parallel execution
Test / parallelExecution := false

// skipping tests when running assembly
assembly / test := {}
