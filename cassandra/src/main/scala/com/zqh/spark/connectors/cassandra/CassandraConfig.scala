package com.zqh.spark.connectors.cassandra

import com.zqh.spark.connectors.{ReadConnectorType, WriteConnectorType}
import org.apache.spark.SparkConf

/**
  * Created by zhengqh on 17/8/30.
  */
@deprecated
class CassandraConfig(val conf: SparkConf) {
  val writeConnector = WriteConnectorType("cassandra")
  val readConnector = ReadConnectorType("cassandra")

  val writePrefix = "write." + writeConnector.`type` + "."
  val readPrefix = "read." + readConnector.`type` + "."

  def setCassandraWriteConf(key: String, value: String) = {
    key match {
      case "host" =>
        conf.set("spark.cassandra.connection.host", value)
      case _ =>
        conf.set(writePrefix + key, value)
    }
  }
  def getCassandraWriteConf(key: String) = conf.get(writePrefix + key)
  def getCassandraWriteConf(key: String, default: String) = conf.get(writePrefix + key, default)

  def setCassandraReadConf(key: String, value: String) = {
    key match {
      case "host" =>
        conf.set("spark.cassandra.connection.host", value)
      case _ =>
        conf.set(readPrefix + key, value)
    }
  }
  def getCassandraReadConf(key: String) = conf.get(readPrefix + key)
  def getCassandraReadConf(key: String, default: String) = conf.get(readPrefix + key, default)
}

@deprecated
object CassandraConfig {
  implicit def cassandraConfig(conf: SparkConf) = new CassandraConfig(conf)
}
