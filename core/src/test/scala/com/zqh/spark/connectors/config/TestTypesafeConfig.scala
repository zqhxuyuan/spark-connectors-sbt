package com.zqh.spark.connectors.config

import com.typesafe.config.ConfigFactory
import scala.collection.JavaConversions._
/**
  * Created by zhengqh on 17/8/31.
  */
object TestTypesafeConfig {

  def main(args: Array[String]) {
    val conf = ConfigFactory.load()
    val readType = conf.getString("reader.type")
    println(readType)

    val pipeConf = ConfigFactory.load("pipeline")
    val readers = pipeConf.getConfig("readers")
    val entries = readers.entrySet()
    for(entry <- entries) {
      println(entry.getKey + ": " + entry.getValue)
    }
    println(readers.getString("jdbc1.table"))

    val connectors = ConfigUtils.loadConfig("complex")
    println(connectors("readers").mkString("\n"))
    println(connectors("writers").mkString("\n"))
  }

}
