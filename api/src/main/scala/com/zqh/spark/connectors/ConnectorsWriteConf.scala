package com.zqh.spark.connectors

import org.apache.spark.SparkConf

/**
  * Created by zhengqh on 17/8/30.
  */
class ConnectorsWriteConf(connector: String) extends SparkConf{

  val prefix = s"write.$connector."

  def setWriteConf(key: String, value: String) = {
    this.set(prefix + key, value)
    this
  }

  def getWriteConf(key: String) = this.get(prefix + key)

  def getWriteConf(key: String, default: String) = this.get(prefix + key, default)
}
