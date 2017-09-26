package com.zqh.spark.connectors.streaming

import com.zqh.spark.connectors.SparkWriter
import com.zqh.spark.connectors.config.ConnectorsWriteConf
import org.apache.spark.sql.DataFrame

/**
  * Created by zhengqh on 17/9/8.
  */
class StreamingWriter(config: ConnectorsWriteConf) extends SparkWriter{
  override def write(df: DataFrame): Unit = {
    val configMap = config.getConfigMap()
    val output = config.get("")
    val trigger = configMap.getOrElse("trigger", 1)

    configMap.get("format") match {
      case Some(writeFormat) =>
        df.writeStream.format(writeFormat).options(configMap).start()
      case None =>
    }
  }
}
