package com.zqh.spark.connectors.cassandra

import com.zqh.spark.connectors.SparkWriter
import com.zqh.spark.connectors.config.ConnectorsWriteConf
import org.apache.spark.sql.DataFrame

/**
  * Created by zhengqh on 17/8/29.
  */
@deprecated
class CassandraWriter(conf: ConnectorsWriteConf) extends SparkWriter{

  override def write(df: DataFrame) = {
    val keyspace = conf.getWriteConf("keyspace")
    val table = conf.getWriteConf("table")
    val writeMode = conf.getWriteConf("mode", "append")

    var cassandraOptions = Map("table" -> table, "keyspace" -> keyspace)
    if(writeMode.toLowerCase.equals("overwrite")) {
      cassandraOptions += "confirm.truncate" -> "true"
    }

    df.write.format("org.apache.spark.sql.cassandra")
      .mode(writeMode)
      .options(cassandraOptions)
      .save()
  }

  override def close(): Unit = {
    println("close cassandra writer")
  }
}
