package com.zqh.spark.connectors.test

import com.zqh.spark.connectors.SparkReader
import com.zqh.spark.connectors.config.ConnectorsReadConf
import org.apache.spark.sql.SparkSession

object TestCassandraBySparkJars {

  def main(args: Array[String]) {
    val conf = new ConnectorsReadConf("cassandra").
      setReadConf("keyspace", "mykeyspace").
      setReadConf("table", "users2")
    conf.set("spark.cassandra.connection.host", "192.168.6.70")

    val spark = SparkSession.builder().master("local").config(conf).getOrCreate()

    val reader = Class.forName("com.zqh.spark.connectors.cassandra.CassandraReader").
      getConstructor(classOf[ConnectorsReadConf]).newInstance(conf).asInstanceOf[SparkReader]

    val df = reader.read(spark)
    df.collect().foreach(println)
  }
}
