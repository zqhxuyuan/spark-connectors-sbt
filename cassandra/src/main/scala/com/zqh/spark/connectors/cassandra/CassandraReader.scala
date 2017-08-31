package com.zqh.spark.connectors.cassandra

import com.zqh.spark.connectors.SparkReader
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import CassandraConfig._

/**
  * Created by zhengqh on 17/8/29.
  */
class CassandraReader(conf: SparkConf) extends SparkReader{

  override def read(spark: SparkSession): DataFrame = {
    val keyspace = conf.getCassandraReadConf("keyspace")
    val table = conf.getCassandraReadConf("table")

    val cassandraOptions = Map("table" -> table, "keyspace" -> keyspace)
    spark.read.
      format("org.apache.spark.sql.cassandra").
      options(cassandraOptions).
      load()
  }
}
