package com.adidas.analytics.util

import org.apache.spark.sql.DataFrame
import scala.collection.JavaConverters._

trait JavaConsumable {

  def algorithm(dataFrames: Vector[DataFrame]): Vector[DataFrame]

  def algorithm(dataFrames: java.util.List[DataFrame]): java.util.List[DataFrame] =
    algorithm(dataFrames.asScala.toVector).asJava

  def algorithm(dataFrame: DataFrame): DataFrame = algorithm(Vector(dataFrame))(0)
}
