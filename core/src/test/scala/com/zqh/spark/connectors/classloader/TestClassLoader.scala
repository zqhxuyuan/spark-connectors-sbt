package com.zqh.spark.connectors.classloader

import java.io.File
import java.net.{URL, URLClassLoader}

import com.zqh.spark.connectors.config.WriteConnectorConfig
import com.zqh.spark.connectors.{SparkReader, ConnectorsReadConf}
import com.zqh.spark.connectors.test.{TestClass2, TestClassToLoader}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}
import ClassConstant._

/**
  * Created by zhengqh on 17/8/31.
  */
class TestClassLoader extends FlatSpec with Matchers with BeforeAndAfter{

  "test load local or remote jar" should "invoke method" in {
    val clazz = ClassLoaderUtil.loadClass(List(apiJarPath), "com.zqh.spark.connectors.test.TestClass")
    val instance = clazz.newInstance.asInstanceOf[TestClassToLoader]
    instance.method()

    // 最原始的做法
    val classLoader = new URLClassLoader(Array(new URL(jdbcJarPath)), Thread.currentThread().getContextClassLoader)
    classLoader.loadClass("com.zqh.spark.connectors.jdbc.ReadJdbc")

    // 封装的做法
    val pluginClassLoader = new PluginClassLoader(Array(new URL(jdbcJarPath)))
    pluginClassLoader.loadClass("com.zqh.spark.connectors.jdbc.ReadJdbc")

    // 更进一步的封装
    ClassLoaderUtil.loadReaderClassFromFile(List(jdbcJarPath), jdbcReadConf, "jdbc", "ReadJdbc")

    ClassLoaderUtil.loadReaderClassFromFile(List(cassJarPath), cassandraConf, "cassandra", "CassandraReader")
  }
}
