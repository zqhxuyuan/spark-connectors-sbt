package com.zqh.spark.connectors.jdbc

import com.zqh.spark.connectors.NothingTransformer
import com.zqh.spark.connectors.test.TestSparkConnectors
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import JdbcConfig._

/**
  * Created by zhengqh on 17/8/29.
  *
  * create table test3 select * from test;
  * truncate test3;
  */
object TestJdbcReaderWriter {

  def main(args: Array[String]) {
    val conf = new SparkConf().
      setJdbcReadConf("url", "jdbc:mysql://localhost/test").
      setJdbcReadConf("table", "test").
      setJdbcReadConf("user", "root").
      setJdbcReadConf("password", "root").
      setJdbcWriteConf("url", "jdbc:mysql://localhost/test").
      setJdbcWriteConf("table", "test2").
      setJdbcWriteConf("user", "root").
      setJdbcWriteConf("password", "root").
      setJdbcWriteConf("mode", "overwrite")

    val spark = SparkSession.builder().master("local").config(conf).getOrCreate()

    val reader = new JdbcReader(conf)
    val writer = new JdbcWriter(conf)
    val transformer = new NothingTransformer

    val connector = new TestSparkConnectors(reader, writer, transformer, spark)
    connector.runSparkJob()
  }
}

