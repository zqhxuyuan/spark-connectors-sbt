package com.zqh.spark.connectors.test

import com.zqh.spark.connectors.{ConnectorsReadConf, SparkReader}
import org.apache.spark.sql.SparkSession


/**
  * Created by zhengqh on 17/8/31.
  *
  * 1. sbt cassandra/assembly -Denv=provided
  * 2. sbt core/assembly -Denv=provided
  * 3. run spark-submit
  *
  bin/spark-submit \
  --jars /Users/zhengqh/spark-connectors-sbt/cassandra/target/scala-2.11/cassandra-assembly-0.0.1.jar \
  --class com.zqh.spark.connectors.test.TestCassandraBySparkJars \
  /Users/zhengqh/spark-connectors-sbt/core/target/scala-2.11/core-assembly-0.0.1.jar
  */
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
