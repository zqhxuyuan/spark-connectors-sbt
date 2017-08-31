package com.zqh.spark.connectors.jdbc

import com.zqh.spark.connectors.NothingTransformer
import com.zqh.spark.connectors.test.{TestSparkConnectors, TestSparkWriter}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.zqh.spark.connectors.jdbc.JdbcConfig._

/**
  * Created by zhengqh on 17/8/29.
  */
object TestJdbcReader {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setJdbcReadConf("url", "jdbc:mysql://localhost/test")
      .setJdbcReadConf("table", "test")
      .setJdbcReadConf("user", "root")
      .setJdbcReadConf("password", "root")

    val spark = SparkSession.builder().master("local").config(conf).getOrCreate()

    val reader = new JdbcReader(conf)
    val writer = new TestSparkWriter()
    val transformer = new NothingTransformer

    val connector = new TestSparkConnectors(reader, writer, transformer, spark)
    connector.runSparkJob()
  }
}

