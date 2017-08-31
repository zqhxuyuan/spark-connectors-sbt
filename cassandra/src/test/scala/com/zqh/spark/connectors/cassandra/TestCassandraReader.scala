package com.zqh.spark.connectors.cassandra

import com.zqh.spark.connectors.ConnectorsReadConf
import com.zqh.spark.connectors.test.{TestSparkConnectors, TestSparkWriter}
import org.apache.spark.sql.SparkSession

/**
  * Created by zhengqh on 17/8/29.
  */
object TestCassandraReader {

  def main(args: Array[String]) {
    val conf = new ConnectorsReadConf("cassandra").
      setReadConf("keyspace", "mykeyspace").
      setReadConf("table", "users2").
      setReadConf("host", "192.168.6.70")

    val reader = new CassandraReader(conf)
    val writer = new TestSparkWriter()

    val spark = SparkSession.builder().master("local").config(conf).getOrCreate()

    val connector = new TestSparkConnectors(reader, writer, spark)
    connector.runSparkJob()
  }
}
