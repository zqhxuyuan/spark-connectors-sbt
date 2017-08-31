package com.zqh.spark.connectors.pipeline

import com.zqh.spark.connectors.jdbc.JdbcConfig._
import com.zqh.spark.connectors.jdbc.{JdbcReader, JdbcWriter}
import com.zqh.spark.connectors.test.{TestSparkPipelines, TestSparkReader2, TestSparkWriter}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by zhengqh on 17/8/30.
  */
object TestSparkPipelineApp {

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().
      setJdbcReadConf("url", "jdbc:mysql://localhost/test").
      setJdbcReadConf("table", "test").
      setJdbcReadConf("user", "root").
      setJdbcReadConf("password", "root").
      setJdbcWriteConf("url", "jdbc:mysql://localhost/test").
      setJdbcWriteConf("table", "test3").
      setJdbcWriteConf("user", "root").
      setJdbcWriteConf("password", "root")

    val spark = SparkSession.builder().master("local").config(conf).getOrCreate()

    val reader = new TestSparkReader2()
    val writer = new TestSparkWriter()

    val jdbcReader = new JdbcReader(conf)
    val jdbcWriter = new JdbcWriter(conf)

    // 并行测试
    val pipeline = new TestSparkPipelines(
      List(reader, jdbcReader),
      List(writer, jdbcWriter), spark
    )
    pipeline.runSparkJob()
  }
}
