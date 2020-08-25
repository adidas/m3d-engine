package com.adidas.analytics.config.shared

import com.adidas.analytics.util.ConfigReader
import scala.util.parsing.json.{JSON, JSONObject}

trait DataReshapingTaskConfig {

  protected def configReader: ConfigReader

  protected val additonalTask: Option[Map[String, String]] =
    configReader.getAsOption[JSONObject]("additional_task") match {
      case Some(value) => JSON.parseFull(value.toString()).asInstanceOf[Option[Map[String, String]]]
      case _           => None
    }

  protected val flattenTaskProperties: Option[Map[String, String]] =
    getAdditionalSetting[Map[String, String]]("nested_task_properties", additonalTask)

  protected val transposeTaskProperties: Option[Map[String, String]] =
    getAdditionalSetting[Map[String, String]]("transpose_task_properties", additonalTask)

  protected val enforceSchema: Boolean =
    getAdditionalSetting[Boolean]("enforce_schema", additonalTask).getOrElse(false)

  protected def getAdditionalSetting[T](
      propertyName: String,
      optSetting: Option[Map[String, String]]
  ): Option[T] =
    optSetting match {
      case Some(x) => x.get(propertyName).asInstanceOf[Option[T]]
      case _       => None
    }

  protected def getProperties[T](propertyName: String, secondProperty: String): Option[T] = {
    val property = Some(additonalTask.get(propertyName).asInstanceOf[Map[String, String]]) match {
      case Some(x) => x.get(secondProperty).asInstanceOf[Option[T]]
      case _       => None
    }
    property
  }

  protected def getAdditionalTaskProperty[T](option: Option[T]): T =
    option.getOrElse(throw new RuntimeException(s"$option value is missing"))

}
