package com.zqh.spark.connectors.cassandra

import com.zqh.spark.connectors.SparkReader
import com.zqh.spark.connectors.config.ConnectorsReadConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by zhengqh on 17/8/29.
  */
@deprecated
class CassandraReader(conf: ConnectorsReadConf) extends SparkReader{

  override def init(spark: SparkSession) = {
    println("init cassandra reader...")
  }

  override def read(spark: SparkSession): DataFrame = {
    val keyspace = conf.getReadConf("keyspace")
    val table = conf.getReadConf("table")

    val cassandraOptions = Map("table" -> table, "keyspace" -> keyspace)
    spark.read.
      format("org.apache.spark.sql.cassandra").
      options(cassandraOptions).
      load()
  }
}
