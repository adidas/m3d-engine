package com.adidas.analytics.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.tools.{DistCp, DistCpOptions}

import scala.collection.JavaConverters._

class DistCpWrapper(conf: Configuration, sources: Seq[Path], target: Path) {

  def run(mapsNum: Int = 10, atomic: Boolean = false, overwrite: Boolean = false): Job = {
    val baseOptions = new DistCpOptions.Builder(sources.asJava, target)
      .withAppend(false)
      .withBlocking(true)
      .withSyncFolder(false)
      .withDeleteMissing(false)
      .maxMaps(mapsNum)
      .withOverwrite(overwrite)
      .withAtomicCommit(atomic)

    new DistCp(conf, baseOptions.build()).execute()
  }
}

object DistCpWrapper {

  def apply(conf: Configuration, sources: Seq[Path], target: Path): DistCpWrapper =
    new DistCpWrapper(conf, sources, target)
}
