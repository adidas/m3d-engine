package com.adidas.analytics.config.shared

import com.adidas.analytics.algo.core.Metadata
import com.adidas.analytics.util.{SparkRecoverPartitionsCustom, SparkRecoverPartitionsNative}

trait MetadataUpdateStrategy extends ConfigurationContext {

  protected def getMetaDataUpdateStrategy(targetTable: String,
                                          partitionColumns: Seq[String]): Metadata =
    configReader.getAsOption[String]("metadata_update_strategy") match {
      case Some("SparkRecoverPartitionsNative") => SparkRecoverPartitionsNative(targetTable, partitionColumns)
      case Some("SparkRecoverPartitionsCustom") => SparkRecoverPartitionsCustom(targetTable, partitionColumns)
      case Some(invalidConfig) => throw new Exception(s"Invalid metadata update strategy ${invalidConfig}")
      case None => SparkRecoverPartitionsNative(targetTable, partitionColumns)
    }

}
