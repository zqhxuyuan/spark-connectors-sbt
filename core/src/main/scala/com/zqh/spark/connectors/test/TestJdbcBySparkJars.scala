package com.zqh.spark.connectors.test

import com.zqh.spark.connectors.SparkReader
import com.zqh.spark.connectors.config.ConnectorsReadConf
import org.apache.spark.sql.SparkSession

object TestJdbcBySparkJars {

  def main(args: Array[String]) {
    val conf = new ConnectorsReadConf("jdbc")
      .setReadConf("url", "jdbc:mysql://localhost/test")
      .setReadConf("table", "test")
      .setReadConf("user", "root")
      .setReadConf("password", "root")

    val spark = SparkSession.builder().master("local").config(conf).getOrCreate()

    val reader = Class.forName("com.zqh.spark.connectors.jdbc.ReadJdbc").
      getConstructor(classOf[ConnectorsReadConf]).newInstance(conf).asInstanceOf[SparkReader]

    val df = reader.read(spark)
    df.collect().foreach(println)
  }
}
