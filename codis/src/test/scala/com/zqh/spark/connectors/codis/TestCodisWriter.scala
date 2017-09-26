package com.zqh.spark.connectors.codis

import com.zqh.spark.connectors.config.{ConnectorsWriteConf, ConnectorsReadConf}
import com.zqh.spark.connectors.NothingTransformer
import com.zqh.spark.connectors.test.{TestCodisReader, TestSparkConnectors, TestSparkReaderUser}
import org.apache.spark.sql.SparkSession

/**
  * Created by zhengqh on 17/9/7.
  */
object TestCodisWriter {

  def main(args: Array[String]) {
    val conf = new ConnectorsWriteConf("codis").
      setWriteConf("zkHost", "192.168.6.55:2181,192.168.6.56:2181,192.168.6.57:2181").
      setWriteConf("zkDir", "/zk/codis/db_tongdun_codis_test/proxy").
      setWriteConf("password", "tongdun123").
      setWriteConf("command", "lpush")

    val spark = SparkSession.builder().master("local").getOrCreate()

    val readConf = new ConnectorsReadConf("codis").
      setReadConf("op", "lpush")

    val reader = new TestCodisReader(readConf)
    val writer = new CodisWriter(conf)
    val transformer = new NothingTransformer

    val connector = new TestSparkConnectors(reader, writer, transformer, spark)
    connector.runSparkJob()
  }
}
