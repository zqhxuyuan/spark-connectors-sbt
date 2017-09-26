package com.zqh.spark.connectors.classloader

import com.zqh.spark.connectors.config.ConnectorsWriteConf
import com.zqh.spark.connectors.classloader.ClassLoaderUtil._
import com.zqh.spark.connectors.config.{ConnectorsWriteConf, ConnectorsReadConf}
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
  val codisJarPath = projectRoot + "codis/target/scala-2.11/codis-assembly-0.0.1.jar"

  val testClassName = "com.zqh.spark.connectors.test.TestClass2"
  val writeConnectorClass = "com.zqh.spark.connectors.config.WriteConnectorConfig"
  val jdbcClassName = "com.zqh.spark.connectors.jdbc.ReadJdbc"

  // jdbc conf
  val jdbcReadConf = new ConnectorsReadConf("jdbc")
    .setReadConf("url", "jdbc:mysql://localhost/test")
    .setReadConf("table", "test")
    .setReadConf("user", "root")
    .setReadConf("password", "root")

  val jdbcWriteConf = new ConnectorsWriteConf("jdbc")
    .setWriteConf("url", "jdbc:mysql://localhost/test")
    .setWriteConf("table", "test")
    .setWriteConf("user", "root")
    .setWriteConf("password", "root")

  // cassandra conf
  val cassandraReadConf: ConnectorsReadConf = new ConnectorsReadConf("cassandra").
    setReadConf("keyspace", "mykeyspace").
    setReadConf("table", "users2")

  val cassandraWriteConf: ConnectorsWriteConf = new ConnectorsWriteConf("cassandra").
    setWriteConf("keyspace", "mykeyspace").
    setWriteConf("table", "users")

  // codis conf
  val codisWriteConf = new ConnectorsWriteConf("codis").
    setWriteConf("zkHost", "192.168.6.55:2181,192.168.6.56:2181,192.168.6.57:2181").
    setWriteConf("zkDir", "/zk/codis/db_tongdun_codis_test/proxy").
    setWriteConf("password", "tongdun123").
    setWriteConf("command", "lpush")

  val testCodisReadConf = new ConnectorsReadConf("codis").
    setReadConf("op", "lpush")

  val spark = SparkSession.builder().master("local").getOrCreate()

}
