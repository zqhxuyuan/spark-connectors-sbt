package com.zqh.spark.connectors.classloader

import java.io.File
import java.net.URLClassLoader

import com.zqh.spark.connectors.{SparkReader, ConnectorsReadConf}
import com.zqh.spark.connectors.test.TestClassToLoader
import org.apache.spark.sql.SparkSession


/**
  * Created by zhengqh on 17/8/31.
  */
object TestClassLoader {

  def main(args: Array[String]) {
    testLoadCassandra
  }

  def testLoadBasic(): Unit = {
    val api = "/Users/zhengqh/spark-connectors-sbt/api/target/scala-2.11/api-assembly-0.0.1.jar"
    val clazz = ClassLoaderUtil.loadClassFromJar(List(api),
      "com.zqh.spark.connectors.test.TestClass")
    val instance = clazz.newInstance.asInstanceOf[TestClassToLoader]
    instance.method()
  }

  def testLoadJdbc(): Unit = {
    val jdbc = "/Users/zhengqh/Github/spark-connectors/jdbc/target/jdbc-1.0-SNAPSHOT-jar-with-dependencies.jar"
    val jdbcReadConf = new ConnectorsReadConf("jdbc")
      .setReadConf("url", "jdbc:mysql://localhost/test")
      .setReadConf("table", "test")
      .setReadConf("user", "root")
      .setReadConf("password", "root")
    val reader = ClassLoaderUtil.loadReaderClassFromFile(List(jdbc), jdbcReadConf, "jdbc", "ReadJdbc")
  }

  def testLoadCassandra(): Unit = {
    val cassandraConf: ConnectorsReadConf = new ConnectorsReadConf("cassandra").
      setReadConf("keyspace", "mykeyspace").
      setReadConf("table", "users2")
    cassandraConf.set("spark.cassandra.connection.host", "192.168.6.70")
    val spark = SparkSession.builder().master("local").config(cassandraConf).getOrCreate()

    // dynamic class loader
    val cassandra = "/Users/zhengqh/spark-connectors-sbt/cassandra/target/scala-2.11/cassandra-assembly-0.0.1.jar"

    val reader = ClassLoaderUtil.loadReaderClassFromFile(List(cassandra), cassandraConf, "cassandra", "CassandraReader")

  }
}
