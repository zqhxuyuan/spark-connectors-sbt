package com.zqh.spark.connectors.jdbc

import com.zqh.spark.connectors.test.{TestSparkConnectors, TestSparkReader2}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.zqh.spark.connectors.jdbc.JdbcConfig._

/**
  * Created by zhengqh on 17/8/29.
  */
object TestJdbcWriter {

  def main(args: Array[String]) {
    val conf = new SparkConf().
      setJdbcWriteConf("url", "jdbc:mysql://localhost/test").
      setJdbcWriteConf("table", "test").
      setJdbcWriteConf("user", "root").
      setJdbcWriteConf("password", "root")

    val spark = SparkSession.builder().master("local").getOrCreate()

    val reader = new TestSparkReader2()
    val writer = new JdbcWriter(conf)

    val connector = new TestSparkConnectors(reader, writer, spark)
    connector.runSparkJob()
  }
}

