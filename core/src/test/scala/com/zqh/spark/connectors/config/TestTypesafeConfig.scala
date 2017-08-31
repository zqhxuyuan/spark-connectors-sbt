package com.zqh.spark.connectors.config

import com.typesafe.config.ConfigFactory

/**
  * Created by zhengqh on 17/8/31.
  */
object TestTypesafeConfig {

  def main(args: Array[String]) {
    val conf = ConfigFactory.load()
    val readType = conf.getString("reader.type")
    println(readType)
  }
}
