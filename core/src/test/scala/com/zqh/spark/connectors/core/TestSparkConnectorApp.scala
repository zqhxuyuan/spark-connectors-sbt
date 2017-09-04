package com.zqh.spark.connectors.core

import com.zqh.spark.connectors.test.ConsoleSparkWriter
import com.zqh.spark.connectors.test.{ConsoleSparkWriter, TestSparkReader}
import org.apache.spark.sql.SparkSession

/**
  * Created by zhengqh on 17/8/29.
  */
object TestSparkConnectorApp {

  def main(args: Array[String]) {
    val reader = new TestSparkReader()
    val writer = new ConsoleSparkWriter()

    val spark = SparkSession.builder().master("local").getOrCreate()

    val source = reader.read(spark)
    writer.write(source)
  }
}

