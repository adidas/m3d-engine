package com.adidas.analytics.config.shared

import com.adidas.analytics.util.DFSWrapper._
import com.adidas.analytics.util.{ConfigReader, DFSWrapper}
import org.apache.hadoop.fs.Path


trait ConfigurationContext extends Serializable {

  protected def dfs: DFSWrapper
  protected def configLocation: String

  protected lazy val configReader: ConfigReader = {
    val configFilePath = new Path(configLocation)
    val jsonContent = dfs.getFileSystem(configFilePath).readFile(configFilePath)
    ConfigReader(jsonContent)
  }
}
