package com.zqh.spark.connectors.classloader

import java.io.File

/**
  * Created by zhengqh on 17/8/31.
  */
object TestPluginClassLoader {

  def main(args: Array[String]) {
    val jar = "/Users/zhengqh/spark-connectors-sbt/jdbc/target/scala-2.11/jdbc-assembly-0.0.1.jar"
    val url = new File(jar).toURI.toURL
    val pluginClassLoader = new PluginClassLoader(Array(url))

    pluginClassLoader.loadClass("com.zqh.spark.connectors.jdbc.ReadJdbc")
  }
}
