package com.adidas.analytics.config.shared

trait DateComponentDerivationConfiguration {

  protected def partitionSourceColumn: String

  protected def partitionSourceColumnFormat: String

  protected def targetPartitions: Seq[String]
}
