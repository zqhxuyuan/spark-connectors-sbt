package com.zqh.spark.connectors.codis

import com.zqh.spark.connectors.{ConnectorsWriteConf, SparkWriter}
import org.apache.spark.sql.DataFrame

/**
  * Created by zhengqh on 17/8/29.
  */
class CodisWriter(conf: ConnectorsWriteConf) extends SparkWriter{

  override def write(df: DataFrame) = {
    val zkHost = conf.getWriteConf("zkHost")
    val zkDir = conf.getWriteConf("zkDir")
    val password = conf.getWriteConf("password")
    val filter = conf.getWriteConf("filter")
    val ttl = conf.getWriteConf("ttl")
    val command = conf.getWriteConf("command")

    var configMap = Map(
      "zkHost" -> zkHost,
      "zkDir" -> zkDir,
      "password" -> password,
      "filter" -> filter,
      "ttl" -> ttl,
      "command" -> command
    )

    df.write.format("com.zqh.spark.connectors.codis")
      .options(configMap)
      .save()
  }

  override def close(): Unit = {
    println("close codis writer")
  }
}
