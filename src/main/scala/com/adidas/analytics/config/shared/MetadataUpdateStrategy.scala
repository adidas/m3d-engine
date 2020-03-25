package com.adidas.analytics.config.shared

import com.adidas.analytics.algo.core.Metadata
import com.adidas.analytics.util.{RecoverPartitionsCustom, RecoverPartitionsNative}

trait MetadataUpdateStrategy extends ConfigurationContext {

  protected def getMetaDataUpdateStrategy(targetTable: String,
                                          partitionColumns: Seq[String]): Metadata =
    configReader.getAsOption[String]("metadata_update_strategy") match {
      case Some("SparkRecoverPartitionsNative") => RecoverPartitionsNative(targetTable, partitionColumns)
      case Some("SparkRecoverPartitionsCustom") => RecoverPartitionsCustom(targetTable, partitionColumns)
      case Some(invalidConfig) => throw new Exception(s"Invalid metadata update strategy ${invalidConfig}")
      case None => RecoverPartitionsNative(targetTable, partitionColumns)
    }

}
