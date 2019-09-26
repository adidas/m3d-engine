package com.adidas.analytics.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.tools.{DistCp, DistCpOptions}

import scala.collection.JavaConversions._


class DistCpWrapper(conf: Configuration, sources: Seq[Path], target: Path) {

  private val baseOptions = new DistCpOptions(sources, target)

  def run(mapsNum: Int = 10, atomic: Boolean = false, overwrite: Boolean = false): Job = {
    val options = new DistCpOptions(baseOptions)
    options.setAppend(false)
    options.setBlocking(true)
    options.setSyncFolder(false)
    options.setDeleteMissing(false)

    options.setMaxMaps(mapsNum)
    options.setOverwrite(overwrite)
    options.setAtomicCommit(atomic)

    new DistCp(conf, options).execute()
  }
}

object DistCpWrapper {

  def apply(conf: Configuration, sources: Seq[Path], target: Path): DistCpWrapper = {
    new DistCpWrapper(conf, sources, target)
  }
}