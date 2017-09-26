package com.zqh.spark.connectors.jdbc2

import com.zqh.spark.connectors.config.ConnectorsWriteConf
import com.zqh.spark.connectors.jdbc.WriteJdbc
import com.zqh.spark.connectors.NothingTransformer
import com.zqh.spark.connectors.test.{TestSparkConnectors, TestSparkReader2}
import org.apache.spark.sql.SparkSession

/**
  * Created by zhengqh on 17/8/29.
  */
object TestJdbcWriterConf {

  def main(args: Array[String]) {
    val conf = new ConnectorsWriteConf("jdbc").
      setWriteConf("url", "jdbc:mysql://localhost/test").
      setWriteConf("table", "test").
      setWriteConf("user", "root").
      setWriteConf("password", "root")

    val spark = SparkSession.builder().master("local").getOrCreate()

    val reader = new TestSparkReader2()
    val writer = new WriteJdbc(conf)
    val transformer = new NothingTransformer

    val connector = new TestSparkConnectors(reader, writer, transformer, spark)
    connector.runSparkJob()
  }
}

