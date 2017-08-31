package com.zqh.spark.connectors.jdbc

import java.util.Properties

import com.zqh.spark.connectors.SparkWriter
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import JdbcConfig._
/**
  * Created by zhengqh on 17/8/29.
  */
class JdbcWriter(conf: SparkConf) extends SparkWriter{

  override def write(df: DataFrame) = {
    val url = conf.getJdbcWriteConf("url")
    val table = conf.getJdbcWriteConf("table")
    val username = conf.getJdbcWriteConf("user")
    val password = conf.getJdbcWriteConf("password")

    val properties = new Properties
    properties.put("user", username)
    properties.put("password", password)

    val writeMode = conf.getJdbcWriteConf("mode", "append")

    df.write.mode(writeMode).jdbc(url, table, properties)
  }
}
