package com.zqh.spark.connectors.cassandra

import com.zqh.spark.connectors.cassandra.CassandraConfig._
import com.zqh.spark.connectors.test.{TestSparkConnectors, TestSparkReaderUser}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by zhengqh on 17/8/29.
  */
object TestCassandraWriter {

  def main(args: Array[String]) {
    val conf = new SparkConf().
      setCassandraWriteConf("keyspace", "mykeyspace").
      setCassandraWriteConf("table", "users2").
      setCassandraWriteConf("host", "192.168.6.70")

    val spark = SparkSession.builder().master("local").config(conf).getOrCreate()

    val reader = new TestSparkReaderUser()
    val writer = new CassandraWriter(conf)

    val connector = new TestSparkConnectors(reader, writer, spark)
    connector.runSparkJob()
  }
}
