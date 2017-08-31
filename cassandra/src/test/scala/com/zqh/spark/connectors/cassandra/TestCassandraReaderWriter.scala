package com.zqh.spark.connectors.cassandra

import com.zqh.spark.connectors.cassandra.CassandraConfig._
import com.zqh.spark.connectors.test.TestSparkConnectors
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by zhengqh on 17/8/29.
  */
object TestCassandraReaderWriter {

  def main(args: Array[String]) {
    val conf = new SparkConf().
      setCassandraReadConf("keyspace", "mykeyspace").
      setCassandraReadConf("table", "users").
      setCassandraReadConf("host", "192.168.6.70").
      setCassandraWriteConf("keyspace", "mykeyspace").
      setCassandraWriteConf("table", "users2").
      setCassandraWriteConf("host", "192.168.6.70").
      setCassandraWriteConf("mode", "overwrite")

    val spark = SparkSession.builder().master("local").config(conf).getOrCreate()

    val reader = new CassandraReader(conf)
    val writer = new CassandraWriter(conf)

    val connector = new TestSparkConnectors(reader, writer, spark)
    connector.runSparkJob()
  }
}
