import sbt.ExclusionRule

name := "m3d-engine"
version := "1.0"

scalaVersion := "2.11.12"
semanticdbEnabled := true
semanticdbVersion := scalafixSemanticdb.revision
scalacOptions += "-Ywarn-unused-import"

val sparkVersion = "2.4.4"
val hadoopVersion = "2.8.5"

conflictManager := sbt.ConflictManager.latestRevision

mainClass in Compile := Some("com.adidas.analytics.AlgorithmFactory")

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
libraryDependencies += "org.apache.hadoop" % "hadoop-distcp" % hadoopVersion % Provided

libraryDependencies += "joda-time" % "joda-time" % "2.9.3" % Provided
libraryDependencies += "org.joda" % "joda-convert" % "2.2.1"

libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.30"

libraryDependencies += "io.delta" %% "delta-core" % "0.6.1"

/* =====================
 * Dependencies for test
 * ===================== */

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.1" % Test

libraryDependencies +=
  "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion % Test classifier "tests" withExclusions Vector(
    ExclusionRule("io.netty", "netty-all")
  )
libraryDependencies +=
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % Test classifier "tests" withExclusions Vector(
    ExclusionRule("io.netty", "netty-all")
  )

fork in Test := true

// disable parallel execution
parallelExecution in Test := false

// skipping tests when running assembly
test in assembly := {}
