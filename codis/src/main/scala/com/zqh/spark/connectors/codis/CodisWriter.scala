package com.zqh.spark.connectors.codis

import com.zqh.spark.connectors.SparkWriter
import com.zqh.spark.connectors.config.ConnectorsWriteConf
import org.apache.spark.sql.DataFrame

/**
  * Created by zhengqh on 17/8/29.
  */
class CodisWriter(conf: ConnectorsWriteConf) extends SparkWriter{

  override def write(df: DataFrame) = {
    import com.zqh.spark.connectors.ConnectorParameters.Codis._

    val zkHost = conf.getWriteConf(codisZkHost)
    val zkDir = conf.getWriteConf(codisZkDir)
    val password = conf.getWriteConf(codisPassword)
    val filter = conf.getWriteConf(codisFilter)
    val ttl = conf.getWriteConf(codisTTL)
    val command = conf.getWriteConf(codisCommand)

    var configMap = Map(
      codisZkHost -> zkHost,
      codisZkDir -> zkDir,
      codisPassword -> password,
      codisFilter -> filter,
      codisTTL -> ttl,
      codisCommand -> command
    )

    df.write.format("com.zqh.spark.connectors.codis")
      .options(configMap)
      .save()
  }

  override def close(): Unit = {
    println("close codis writer")
  }
}
