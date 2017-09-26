package com.zqh.spark.connectors.mongo

import com.zqh.spark.connectors.WriteConnectorType
import com.zqh.spark.connectors.format.{WriteConnectorType, ReadConnectorType}
import org.apache.spark.SparkConf

/**
  * Created by zhengqh on 17/8/30.
  */
@deprecated
class MongoConfig(conf: SparkConf) {
  val writeConnector = WriteConnectorType("mongo")
  val readConnector = ReadConnectorType("mongo")

  val writePrefix = "write." + writeConnector.`type` + "."
  val readPrefix = "read." + readConnector.`type` + "."

  def setMongoWriteConf(key: String, value: String) = conf.set(writePrefix + key, value)
  def getMongoWriteConf(key: String) = conf.get(writePrefix + key)
  def getMongoWriteConf(key: String, default: String) = conf.get(writePrefix + key, default)

  def setMongoReadConf(key: String, value: String) = conf.set(readPrefix + key, value)
  def getMongoReadConf(key: String) = conf.get(readPrefix + key)
  def getMongoReadConf(key: String, default: String) = conf.get(readPrefix + key, default)
}

@deprecated
object MongoConfig {
  implicit def config(conf: SparkConf) = new MongoConfig(conf)
}
