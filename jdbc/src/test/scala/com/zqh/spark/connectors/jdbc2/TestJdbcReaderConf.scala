package com.zqh.spark.connectors.jdbc2

import com.zqh.spark.connectors.jdbc.ReadJdbc
import com.zqh.spark.connectors.ConnectorsReadConf
import com.zqh.spark.connectors.test.{TestSparkConnectors, TestSparkWriter}
import org.apache.spark.sql.SparkSession

/**
  * Created by zhengqh on 17/8/29.
  */
object TestJdbcReaderConf {

  def main(args: Array[String]) {
    val jdbcReadConf = new ConnectorsReadConf("jdbc")
      .setReadConf("url", "jdbc:mysql://localhost/test")
      .setReadConf("table", "test")
      .setReadConf("user", "root")
      .setReadConf("password", "root")

    val spark = SparkSession.builder().master("local").config(jdbcReadConf).getOrCreate()

    val reader = new ReadJdbc(jdbcReadConf)
    val writer = new TestSparkWriter()

    val connector = new TestSparkConnectors(reader, writer, spark)
    connector.runSparkJob()
  }
}

