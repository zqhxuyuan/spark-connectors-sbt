package com.zqh.spark.connectors.config

import com.zqh.spark.connectors.{WriteConnectorType, ReadConnectorType}
import org.apache.spark.SparkConf

/**
  * Created by zhengqh on 17/8/29.
  */
class WriteConnectorConfig(connector: String, conf: SparkConf) {
  val prefix = s"write.$connector."

  def setWriteConf(key: String, value: String) = conf.set(prefix + key, value)

  def getWriteConf(key: String) = conf.get(prefix + key)

  def getWriteConf(key: String, default: String) = conf.get(prefix + key, default)

}

class ReadConnectorConfig(connector: String, conf: SparkConf) {
  val prefix = s"read.$connector."

  def setReadConf(key: String, value: String) = conf.set(prefix + key, value)

  def getReadConf(key: String) = conf.get(prefix + key)

  def getReadConf(key: String, default: String) = conf.get(prefix + key, default)
}




