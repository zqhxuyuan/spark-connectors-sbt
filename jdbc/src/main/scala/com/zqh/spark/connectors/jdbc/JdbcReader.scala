package com.zqh.spark.connectors.jdbc

import java.util.Properties

import com.zqh.spark.connectors.SparkReader
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import JdbcConfig._

/**
  * Created by zhengqh on 17/8/29.
  */
class JdbcReader(conf: SparkConf) extends SparkReader{

  override def read(spark: SparkSession): DataFrame = {
    val url = conf.getJdbcReadConf("url")
    val table = conf.getJdbcReadConf("table")
    val username = conf.getJdbcReadConf("user")
    val password = conf.getJdbcReadConf("password")

    val properties = new Properties
    properties.put("user", username)
    properties.put("password", password)

    spark.read.jdbc(url, table, properties)
  }
}
