package com.zqh.spark.connectors.pipeline

import com.zqh.spark.connectors.jdbc.JdbcConfig._
import com.zqh.spark.connectors.jdbc.{JdbcReader, JdbcWriter}
import com.zqh.spark.connectors.test.{TestSparkParallelPipeline, TestSparkPipelines, TestSparkReader2, TestSparkWriter}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by zhengqh on 17/8/30.
  */
object TestSparkPipelineApp {

  def main(args: Array[String]) {
    // jdbc read and jdbc writer
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

    // demo reader and demo writer
    val reader = new TestSparkReader2()
    val writer = new TestSparkWriter()

    val jdbcReader = new JdbcReader(conf)
    val jdbcWriter = new JdbcWriter(conf)

    // pipe line test
    // demo reader + jdbc reader -> demo writer + jdbc writer
    val pipeline = new TestSparkPipelines(
      List(reader, jdbcReader),
      List(writer, jdbcWriter),
      spark
    )
    pipeline.runSparkJob()
  }
}
