package com.zqh.spark.connectors.cassandra

import com.zqh.spark.connectors.SparkWriter
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import CassandraConfig._

/**
  * Created by zhengqh on 17/8/29.
  */
class CassandraWriter(conf: SparkConf) extends SparkWriter{

  override def write(df: DataFrame) = {
    val keyspace = conf.getCassandraWriteConf("keyspace")
    val table = conf.getCassandraWriteConf("table")
    val writeMode = conf.getCassandraWriteConf("mode", "append")

    var cassandraOptions = Map("table" -> table, "keyspace" -> keyspace)
    if(writeMode.toLowerCase.equals("overwrite")) {
      cassandraOptions += "confirm.truncate" -> "true"
    }

    df.write.format("org.apache.spark.sql.cassandra")
      .mode(writeMode)
      .options(cassandraOptions)
      .save()
  }
}
