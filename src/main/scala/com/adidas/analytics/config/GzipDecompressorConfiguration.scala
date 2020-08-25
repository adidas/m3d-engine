package com.adidas.analytics.config

import com.adidas.analytics.config.shared.ConfigurationContext
import org.apache.hadoop.fs.Path

trait GzipDecompressorConfiguration extends ConfigurationContext {
  protected val recursive: Boolean = true
  protected val outputExtension: String = configReader.getAs[String]("format")

  protected val inputDirectoryPath: Path = new Path(configReader.getAs[String]("directory"))

  protected val threadPoolSize: Int = configReader.getAs[Int]("thread_pool_size")
}
