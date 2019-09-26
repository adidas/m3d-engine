package com.adidas.analytics.algo.core

/**
  * This is a generic trait for all the executable algorithms.
  * It should be used when the concept of the algorithm is different from the regular ETL process.
  */
trait JobRunner {

  /**
    * Execute algorithm
    */
  def run(): Unit
}
