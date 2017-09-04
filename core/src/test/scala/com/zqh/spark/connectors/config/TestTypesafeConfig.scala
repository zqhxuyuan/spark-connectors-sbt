package com.zqh.spark.connectors.config

import com.typesafe.config.ConfigFactory
import com.zqh.spark.connectors.ConnectorsReadConf
import com.zqh.spark.connectors.util.Constant
import scala.collection.JavaConversions._
/**
  * Created by zhengqh on 17/8/31.
  */
object TestTypesafeConfig {

  def main(args: Array[String]) {
    val conf = ConfigFactory.load()
    val readType = conf.getString("reader.format")
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
    val readerConfigs = connectors("readers")
    val writerConfigs = connectors("writers")

    val readerConnectors = readerConfigs.map(reader => {
      val readerType = reader(Constant.FORMAT)
      val conf = new ConnectorsReadConf(readerType)
      reader.filterKeys(!_.equals(Constant.FORMAT)).foreach(kv => {
        conf.set(kv._1, kv._2)
        println(kv._1 + ":" + kv._2)
      })
    })

  }

}
