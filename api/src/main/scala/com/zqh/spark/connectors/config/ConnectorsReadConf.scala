package com.zqh.spark.connectors.config

import org.apache.spark.SparkConf

/**
  * Created by zhengqh on 17/8/30.
  */
@deprecated
class ConnectorsReadConf(connector: String) extends SparkConf{
  def enrichKey(key: String) = s"read.$connector.$key"
  var configMap: Map[String, String] = _

  def setConfigMap(configMap: Map[String, String]) = {
    this.configMap = configMap.map{case (k,v) => enrichKey(k) -> v}
  }

  def getConfigMap() = this.configMap.map{case (k,v) => enrichKey(k) -> v}

  def setReadConf(key: String, value: String) = {
    configMap += (enrichKey(key) -> value)
    this
  }

  def getReadConf(key: String, value: String = ""): String = {
    configMap.getOrElse(enrichKey(key), value)
  }
}
