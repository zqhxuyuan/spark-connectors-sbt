package com.zqh.spark.connectors.jdbc2

import com.zqh.spark.connectors.config.{ConnectorsWriteConf, ConnectorsReadConf}
import com.zqh.spark.connectors.jdbc.{ReadJdbc, WriteJdbc}
import com.zqh.spark.connectors.test.TestSparkConnectors
import com.zqh.spark.connectors.NothingTransformer
import org.apache.spark.sql.SparkSession

/**
  * Created by zhengqh on 17/8/29.
  *
  * create table test3 select * from test;
  * truncate test3;
  */
object TestJdbcReaderWriterConf {

  def main(args: Array[String]) {
    val jdbcReadConf = new ConnectorsReadConf("jdbc").
      setReadConf("url", "jdbc:mysql://localhost/test").
      setReadConf("table", "test").
      setReadConf("user", "root").
      setReadConf("password", "root")

    val jdbcWriteConf = new ConnectorsWriteConf("jdbc").
      setWriteConf("url", "jdbc:mysql://localhost/test").
      setWriteConf("table", "test2").
      setWriteConf("user", "root").
      setWriteConf("password", "root").
      setWriteConf("mode", "overwrite")

    val spark = SparkSession.builder().master("local").getOrCreate()

    val reader = new ReadJdbc(jdbcReadConf)
    val writer = new WriteJdbc(jdbcWriteConf)
    val transformer = new NothingTransformer

    val connector = new TestSparkConnectors(reader, writer, transformer, spark)
    connector.runSparkJob()
  }
}

