package com.zqh.spark.connectors.cassandra

import com.zqh.spark.connectors.config.{ConnectorsWriteConf, ConnectorsReadConf}
import com.zqh.spark.connectors.NothingTransformer
import com.zqh.spark.connectors.test.TestSparkConnectors
import org.apache.spark.sql.SparkSession

/**
  * Created by zhengqh on 17/8/29.
  */
object TestCassandraReaderWriter {

  def main(args: Array[String]) {
    val conf = new ConnectorsReadConf("cassandra").
      setReadConf("keyspace", "mykeyspace").
      setReadConf("table", "users").
      setReadConf("host", "192.168.6.70")

    val writeConf = new ConnectorsWriteConf("cassandra").
      setWriteConf("keyspace", "mykeyspace").
      setWriteConf("table", "users2").
      setWriteConf("host", "192.168.6.70").
      setWriteConf("mode", "overwrite")

    val transformer = new NothingTransformer

    val spark = SparkSession.builder().master("local").config(conf).getOrCreate()

    val reader = new CassandraReader(conf)
    val writer = new CassandraWriter(writeConf)

    val connector = new TestSparkConnectors(reader, writer, transformer, spark)
    connector.runSparkJob()
  }
}
