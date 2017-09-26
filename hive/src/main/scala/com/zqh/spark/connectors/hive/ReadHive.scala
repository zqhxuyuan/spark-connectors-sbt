package com.zqh.spark.connectors.hive

import com.zqh.spark.connectors.SparkReader
import com.zqh.spark.connectors.config.ConnectorsReadConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by zhengqh on 17/8/29.
  */
class ReadHive(conf: ConnectorsReadConf) extends SparkReader{

  override def init(spark: SparkSession) = {
    println("init hive reader...")
  }

  override def read(spark: SparkSession): DataFrame = {
    val dbName = conf.getReadConf("db")
    val tableName = conf.getReadConf("table")
    val partition = conf.getReadConf("partition", "")
    val dbTable = dbName + "." + tableName

    spark.read.table(dbTable)
  }
}
