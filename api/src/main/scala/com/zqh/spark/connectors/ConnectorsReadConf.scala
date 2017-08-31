package com.zqh.spark.connectors

import org.apache.spark.SparkConf

/**
  * Created by zhengqh on 17/8/30.
  */
class ConnectorsReadConf(connector: String) extends SparkConf{
  val prefix = s"read.$connector."

  def setReadConf(key: String, value: String) = {
    this.set(prefix + key, value)
    this
  }

  def getReadConf(key: String) = this.get(prefix + key)

  def getReadConf(key: String, default: String) = this.get(prefix + key, default)
}
