package com.adidas.analytics.util

import java.text.DecimalFormatSymbols

import org.slf4j.{Logger, LoggerFactory}

import scala.util.parsing.json.{JSON, JSONArray, JSONObject}

/**
  * Base class capable of reading parameters from config content
  */
class ConfigReader(jsonContent: String) extends Serializable {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private val decimalSeparator: Char = new DecimalFormatSymbols().getDecimalSeparator

  JSON.globalNumberParser = (in: String) => if (in.contains(decimalSeparator)) in.toDouble else in.toInt

  private lazy val config = JSON.parseRaw(jsonContent) match {
    case Some(JSONObject(obj)) => obj
    case _ => throw new IllegalArgumentException(s"Wrong format of the configuration file: $jsonContent")
  }

  def getAsSeq[T](propertyName: String): Seq[T] = {
    config.get(propertyName) match {
      case Some(JSONArray(list)) => list.map(_.asInstanceOf[T])
      case _ => throw new IllegalArgumentException(s"Unable to find configuration property $propertyName")
    }
  }

  def getAsMap[K, V](propertyName: String): Map[K,V] = {
    config.get(propertyName) match {
      case Some(JSONObject(obj)) => obj.asInstanceOf[Map[K,V]]
      case _ => throw new IllegalArgumentException(s"Unable to find configuration property $propertyName")
    }
  }

  def getAs[T](propertyName: String): T = {
    config.get(propertyName) match {
      case Some(property) => property.asInstanceOf[T]
      case None => throw new IllegalArgumentException(s"Unable to find configuration property $propertyName")
    }
  }

  def getAsOption[T](propertyName: String): Option[T] = {
    config.get(propertyName).map(property => property.asInstanceOf[T])
  }

  def getAsOptionSeq[T](propertyName: String): Option[Seq[T]] = {
    config.get(propertyName).map(_ => getAsSeq(propertyName))
  }

  def contains(propertyName: String): Boolean = {
    config.contains(propertyName)
  }
}

object ConfigReader {
  def apply(jsonContent: String): ConfigReader = new ConfigReader(jsonContent)
}