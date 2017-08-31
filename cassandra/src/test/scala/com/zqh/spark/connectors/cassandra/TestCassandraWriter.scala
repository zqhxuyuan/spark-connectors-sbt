package com.zqh.spark.connectors.cassandra

import com.zqh.spark.connectors.{NothingTransformer, ConnectorsWriteConf}
import com.zqh.spark.connectors.test.{TestSparkConnectors, TestSparkReaderUser}
import org.apache.spark.sql.SparkSession

/**
  * Created by zhengqh on 17/8/29.
  */
object TestCassandraWriter {

  def main(args: Array[String]) {

    val conf = new ConnectorsWriteConf("cassandra").
      setWriteConf("keyspace", "mykeyspace").
      setWriteConf("table", "users2").
      setWriteConf("host", "192.168.6.70").
      setWriteConf("mode", "overwrite")

    val spark = SparkSession.builder().master("local").config(conf).getOrCreate()

    val reader = new TestSparkReaderUser()
    val writer = new CassandraWriter(conf)
    val transformer = new NothingTransformer

    val connector = new TestSparkConnectors(reader, writer, transformer, spark)
    connector.runSparkJob()
  }
}
