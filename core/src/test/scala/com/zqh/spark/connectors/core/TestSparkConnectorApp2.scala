package com.zqh.spark.connectors.core

import com.zqh.spark.connectors.NothingTransformer
import com.zqh.spark.connectors.test.{ConsoleSparkWriter, TestSparkReader}
import org.apache.spark.sql.SparkSession

/**
  * Created by zhengqh on 17/8/29.
  */
object TestSparkConnectorApp2 {
  def main(args: Array[String]) {
    val reader = new TestSparkReader()
    val writer = new ConsoleSparkWriter()
    val transformer = new NothingTransformer
    val spark = SparkSession.builder().master("local").getOrCreate()
    val connectors = new SparkConnectors(reader, writer, transformer, spark)
    connectors.runSparkJob()
  }
}
