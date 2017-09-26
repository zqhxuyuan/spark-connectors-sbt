package com.zqh.spark.connectors.streaming

import com.zqh.spark.connectors.SparkReader
import com.zqh.spark.connectors.config.ConnectorsReadConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by zhengqh on 17/9/8.
  */
class StreamingReader(config: ConnectorsReadConf) extends SparkReader{
  override def read(spark: SparkSession): DataFrame = {
    val readFormat = config.getReadConf("format")
    val configMap = config.getConfigMap()

    spark.readStream
      .format(readFormat).options(configMap)
      .load()
  }
}
