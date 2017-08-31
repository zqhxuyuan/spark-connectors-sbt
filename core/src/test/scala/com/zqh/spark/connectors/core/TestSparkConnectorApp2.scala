package com.zqh.spark.connectors.core

import com.zqh.spark.connectors.TestSparkWriter
import com.zqh.spark.connectors.test.{TestSparkWriter, TestSparkReader}
import org.apache.spark.sql.SparkSession

/**
  * Created by zhengqh on 17/8/29.
  */
object TestSparkConnectorApp2 {
  def main(args: Array[String]) {
    val reader = new TestSparkReader()
    val writer = new TestSparkWriter()
    val spark = SparkSession.builder().master("local").getOrCreate()

    val connectors = new SparkConnectors(reader, writer, spark)
    connectors.runSparkJob()
  }
}
