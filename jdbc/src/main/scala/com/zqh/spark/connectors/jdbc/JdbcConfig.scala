package com.zqh.spark.connectors.jdbc

import com.zqh.spark.connectors.{ReadConnectorType, WriteConnectorType}
import org.apache.spark.SparkConf

/**
  * Created by zhengqh on 17/8/30.
  */
@deprecated
class JdbcConfig(val conf: SparkConf) {
  val writeConnector = WriteConnectorType("jdbc")
  val readConnector = ReadConnectorType("jdbc")

  val writePrefix = "write." + writeConnector.`type` + "."
  val readPrefix = "read." + readConnector.`type` + "."

  def setJdbcWriteConf(key: String, value: String) = conf.set(writePrefix + key, value)
  def getJdbcWriteConf(key: String) = conf.get(writePrefix + key)
  def getJdbcWriteConf(key: String, default: String) = conf.get(writePrefix + key, default)

  def setJdbcReadConf(key: String, value: String) = conf.set(readPrefix + key, value)
  def getJdbcReadConf(key: String) = conf.get(readPrefix + key)
  def getJdbcReadConf(key: String, default: String) = conf.get(readPrefix + key, default)
}

@deprecated
object JdbcConfig {
  implicit def jdbcConfig(conf: SparkConf) = new JdbcConfig(conf)
}
