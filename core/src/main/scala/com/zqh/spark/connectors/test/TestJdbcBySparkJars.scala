package com.zqh.spark.connectors.test

import com.zqh.spark.connectors.{SparkReader, ConnectorsReadConf}
import org.apache.spark.sql.SparkSession


/**
  * Created by zhengqh on 17/8/31.
  *
  * 1. sbt jdbc/assembly -Denv=provided
  * 2. sbt core/assembly -Denv=provided
  * 3. run spark-submit
  *
  * Notice:
  *
  * 1. mysql driver jar add to driver class path, not to --jars
  * 2. dependencies jar which is jdbc-assembly.jar add to --jars
  * 3. running jar which is core-assembly is the resources
  *
  bin/spark-submit --driver-class-path /Users/zhengqh/.m2/repository/mysql/mysql-connector-java/5.1.40/mysql-connector-java-5.1.40.jar \
  --jars /Users/zhengqh/spark-connectors-sbt/jdbc/target/scala-2.11/jdbc-assembly-0.0.1.jar \
  --class com.zqh.spark.connectors.test.TestJdbcBySparkJars \
  /Users/zhengqh/spark-connectors-sbt/core/target/scala-2.11/core-assembly-0.0.1.jar

  * Question:
  * 1. mysql driver already exist in jdbc-assembly.jar, and we add jdbc-assembly to --jars,
  * If without adding mysql driver to driver class path, still can't find suitable driver..
  */
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
