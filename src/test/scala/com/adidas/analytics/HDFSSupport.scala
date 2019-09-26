package com.adidas.analytics

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{CommonConfigurationKeysPublic, FileSystem}
import org.apache.hadoop.hdfs.MiniDFSCluster
import org.slf4j.Logger

trait HDFSSupport {

  private lazy val defaultDataNodesNum: Int = 2
  private lazy val defaultPort: Int = 8201

  lazy val cluster: MiniDFSCluster = startHDFS(clusterHdfsConf)
  lazy val fs: FileSystem = cluster.getFileSystem()

  def logger: Logger
  def testAppId: String
  def localTestDir: String
  def clusterHdfsConf: Option[Configuration] = Option.empty

  def startHDFS(hadoopConf: Option[Configuration]): MiniDFSCluster = {
    val appDir = new File(localTestDir, testAppId)
    val hdfsTestDir = new File(appDir, "hdfs").getAbsoluteFile
    hdfsTestDir.mkdirs()

    val clusterConf = hadoopConf.fold(new Configuration())(c => new Configuration(c))
    clusterConf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, hdfsTestDir.getAbsolutePath)
    clusterConf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, s"hdfs://localhost:$defaultPort/")

    logger.info(s"Starting test DFS cluster with base directory at ${hdfsTestDir.getAbsolutePath} ...")
    new MiniDFSCluster.Builder(clusterConf)
      .numDataNodes(defaultDataNodesNum)
      .nameNodePort(defaultPort)
      .format(true)
      .build()
  }
}
