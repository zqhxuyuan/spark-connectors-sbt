package com.zqh.spark.connectors.jdbc

import java.util.Properties

import com.zqh.spark.connectors.{ConnectorsReadConf, SparkReader}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by zhengqh on 17/8/29.
  */
class ReadJdbc(conf: ConnectorsReadConf) extends SparkReader{

  override def init(spark: SparkSession) = {
    println("init jdbc reader...")
  }

  override def read(spark: SparkSession): DataFrame = {
    val url = conf.getReadConf("url")
    val table = conf.getReadConf("table")
    val username = conf.getReadConf("user")
    val password = conf.getReadConf("password")

    val properties = new Properties
    properties.put("user", username)
    properties.put("password", password)

    spark.read.jdbc(url, table, properties)
  }
}
