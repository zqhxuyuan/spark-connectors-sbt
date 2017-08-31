package com.zqh.spark.connectors.classloader

import com.zqh.spark.connectors.ConnectorsReadConf
import com.zqh.spark.connectors.core.SparkConnectors
import com.zqh.spark.connectors.test.TestSparkWriter
import org.apache.spark.sql.SparkSession

/**
  * Created by zhengqh on 17/8/31.
  */
object TestClassLoader {

  def main(args: Array[String]) {
    val jdbcReadConf = new ConnectorsReadConf("jdbc")
      .setReadConf("url", "jdbc:mysql://localhost/test")
      .setReadConf("table", "test")
      .setReadConf("user", "root")
      .setReadConf("password", "root")

    val spark = SparkSession.builder().master("local").config(jdbcReadConf).getOrCreate()

    //SQLException: No suitable driver
    //ClassLoaderUtil.addJarToClassPath(List("/Users/zhengqh/.m2/repository/mysql/mysql-connector-java/5.1.40/mysql-connector-java-5.1.40.jar"))

    // dynamic class loader
    val jdbcJar = "/Users/zhengqh/spark-connectors-sbt/jdbc/target/scala-2.11/jdbc-assembly-0.0.1.jar"
    val reader = ClassLoaderUtil.loadReaderClass(List(jdbcJar), jdbcReadConf, "jdbc", "ReadJdbc")

    // run job
    val writer = new TestSparkWriter()
    val connector = new SparkConnectors(reader, writer, spark)
    connector.runSparkJob()

  }
}
