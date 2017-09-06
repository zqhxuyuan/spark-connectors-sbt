package com.zqh.spark.connectors.classloader

import com.zqh.spark.connectors.ConnectorsReadConf
import com.zqh.spark.connectors.classloader.ClassLoaderUtil._
import org.apache.spark.sql.SparkSession

/**
  * Created by zhengqh on 17/9/6.
  */
object ClassConstant {

  val hdfsPluginJarPath = s"${hdfsSchema}/user/pontus/core-assembly-0.0.1.jar"

  val projectRoot = "file:/Users/zhengqh/spark-connectors-sbt/"
  val filePluginJarPath = projectRoot + "core/target/scala-2.11/core-assembly-0.0.1.jar"

  val coreJarPath = projectRoot + "core/target/scala-2.11/core-assembly-0.0.1.jar"
  val apiJarPath = projectRoot + "api/target/scala-2.11/api-assembly-0.0.1.jar"
  val jdbcJarPath = projectRoot + "jdbc/target/scala-2.11/jdbc-assembly-0.0.1.jar"
  val cassJarPath = projectRoot + "cassandra/target/scala-2.11/cassandra-assembly-0.0.1.jar"

  val testClassName = "com.zqh.spark.connectors.test.TestClass2"
  val writeConnectorClass = "com.zqh.spark.connectors.config.WriteConnectorConfig"
  val jdbcClassName = "com.zqh.spark.connectors.jdbc.ReadJdbc"

  val jdbcReadConf = new ConnectorsReadConf("jdbc")
    .setReadConf("url", "jdbc:mysql://localhost/test")
    .setReadConf("table", "test")
    .setReadConf("user", "root")
    .setReadConf("password", "root")

  val cassandraConf: ConnectorsReadConf = new ConnectorsReadConf("cassandra").
    setReadConf("keyspace", "mykeyspace").
    setReadConf("table", "users2")

  val spark = SparkSession.builder().master("local").getOrCreate()

}
