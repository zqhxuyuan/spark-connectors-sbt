package com.zqh.spark.connectors.cassandra

import com.zqh.spark.connectors.test.{TestSparkConnectors, TestSparkWriter}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import com.zqh.spark.connectors.cassandra.CassandraConfig._

/**
  * Created by zhengqh on 17/8/29.
  */
object TestCassandraReader {

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().
      setCassandraReadConf("keyspace", "mykeyspace").
      setCassandraReadConf("table", "users2").
      setCassandraReadConf("host", "192.168.6.70")

    val config = new CassandraConfig(conf)

    val reader = new CassandraReader(conf)
    val writer = new TestSparkWriter()

    val spark = SparkSession.builder().master("local").config(conf).getOrCreate()

    val connector = new TestSparkConnectors(reader, writer, spark)
    connector.runSparkJob()
  }
}
