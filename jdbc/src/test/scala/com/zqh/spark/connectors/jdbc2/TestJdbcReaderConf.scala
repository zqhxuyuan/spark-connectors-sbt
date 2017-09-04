package com.zqh.spark.connectors.jdbc2

import com.zqh.spark.connectors.jdbc.ReadJdbc
import com.zqh.spark.connectors.{NothingTransformer, ConnectorsReadConf}
import com.zqh.spark.connectors.test.{TestSparkConnectors, ConsoleSparkWriter}
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListenerJobEnd, SparkListenerApplicationEnd, SparkListener}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger

/**
  * Created by zhengqh on 17/8/29.
  */
object TestJdbcReaderConf extends Logging{

  def main(args: Array[String]) {
    val jdbcReadConf = new ConnectorsReadConf("jdbc")
      .setReadConf("url", "jdbc:mysql://localhost/test")
      .setReadConf("table", "test")
      .setReadConf("user", "root")
      .setReadConf("password", "root")

    val spark = SparkSession.builder().master("local").config(jdbcReadConf).getOrCreate()

    spark.sparkContext.addSparkListener(new SparkListener {
      override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) {
        println("spark connectors application end")
        log.info("spark connectors application end")
      }
      override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
        println("spark connectors job end")
        log.info("spark connectors job end")
      }
    })

    val reader = new ReadJdbc(jdbcReadConf)
    val writer = new ConsoleSparkWriter()
    val transformer = new NothingTransformer

    val connector = new TestSparkConnectors(reader, writer, transformer, spark)
    connector.runSparkJob()
  }
}

