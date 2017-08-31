package com.zqh.spark.connectors.classloader

import com.zqh.spark.connectors.{SparkReader, ConnectorsReadConf}
import com.zqh.spark.connectors.test.TestClassToLoader
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


/**
  * Created by zhengqh on 17/8/31.
  */
object TestJdbcBySparkJars {

  def main(args: Array[String]) {
    val root = System.getProperty("user.dir")
    val jdbc = s"$root/jdbc/target/scala-2.11/jdbc-assembly-0.0.1.jar"

    val jdbcReadConf = new ConnectorsReadConf("jdbc")
      .setReadConf("url", "jdbc:mysql://localhost/test")
      .setReadConf("table", "test")
      .setReadConf("user", "root")
      .setReadConf("password", "root")

    //jdbcReadConf.setJars(List(jdbc))
    //jdbcReadConf.set("spark.jars", List(jdbc).mkString(","))
    //jdbcReadConf.set("--jars", List(jdbc).mkString(","))

    val spark = SparkSession.builder().master("local").config(jdbcReadConf).getOrCreate()

    val reader = Class.forName("com.zqh.spark.connectors.jdbc.ReadJdbc").
      getConstructor(classOf[ConnectorsReadConf]).newInstance(jdbcReadConf).asInstanceOf[SparkReader]

    val df = reader.read(spark)
    df.collect().foreach(println)
  }
}
