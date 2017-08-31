package com.zqh.spark.connectors.config

import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._

/**
  * Created by zhengqh on 17/8/31.
  */
object ConfigUtils {

  private def toMap(hashMap: AnyRef): Map[String, AnyRef] = hashMap.asInstanceOf[java.util.Map[String, AnyRef]].toMap
  private def toList(list: AnyRef): List[AnyRef] = list.asInstanceOf[java.util.List[AnyRef]].toList

  def loadConfig(resourceName: String = ""): Map[String, List[Map[String, String]]] = {
    val config = resourceName match {
      case _ if resourceName.equals("") => ConfigFactory.load()
      case _ => ConfigFactory.load(resourceName)
    }

    val connectorsMap: Map[String, List[Map[String, String]]] =
      config.getList("connectors").unwrapped().map { someConfigItem =>
        toMap(someConfigItem) map {
          case (key, value) =>
            key -> toList(value).map {
              x => toMap(x).map { case (k, v) => k -> v.toString }
            }
        }
      }.reduceLeft(_ ++ _)

    connectorsMap
  }

  def loadReaderConfigs(resourceName: String = "") = loadConfig(resourceName).get("readers")

  def loadWriterConfigs(resourceName: String = "") = loadConfig(resourceName).get("writers")
}
