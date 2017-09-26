package com.zqh.spark.connectors.mongo

import com.zqh.spark.connectors.SparkReader
import com.zqh.spark.connectors.config.ConnectorsReadConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by zhengqh on 17/8/29.
  */
class MongoReader(config: ConnectorsReadConf) extends SparkReader{

  override def read(spark: SparkSession): DataFrame = {
    val host = config.getReadConf("host", "localhost")
    val port = config.getReadConf("port", "27017")
    val db = config.getReadConf("db")
    val table = config.getReadConf("table")
    val username = config.getReadConf("user", "")
    val password = config.getReadConf("password", "")
    val id = config.getReadConf("id", "_id")

    val uri = s"mongodb://$host:$port"

    val map = Map(
      "uri" -> uri,
      "database" -> db,
      "collection" -> table
    )

    spark.read.
      format("com.mongodb.spark.sql").
      options(map).
      load()
  }
}
