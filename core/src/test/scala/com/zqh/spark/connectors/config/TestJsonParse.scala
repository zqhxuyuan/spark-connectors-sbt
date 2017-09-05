package com.zqh.spark.connectors.config

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.{FlatSpec, Matchers}
import scala.collection.JavaConversions._

/**
  * Created by zhengqh on 17/9/4.
  */
class TestJsonParse extends FlatSpec with Matchers{

  val configStr = ConfigUtils.loadConfigFile2String("strategy.v2.json")

  "jons-lib" should "parse json" in {
    import net.sf.json.JSONObject
    val jSONObject = JSONObject.fromObject(configStr)
    val map = jSONObject.asInstanceOf[java.util.Map[String, java.util.Map[_, _]]].toMap
    println(map("convert-multi-csv-to-json2"))
  }

  "fastjson" should "parse json" in {
    import com.alibaba.fastjson.JSON

    val map = JSON.parse(configStr).asInstanceOf[java.util.Map[String, java.util.Map[String, Any]]].toMap
    println(map("convert-multi-csv-to-json2"))
  }

  "fastjson inner map" should "parse inner map" in {
    import com.alibaba.fastjson.JSON

    val map = JSON.parse(configStr).asInstanceOf[java.util.Map[String, java.util.Map[String, Any]]].toMap
    val scalaMap = ConfigUtils.javaMap2Map(map)
    val jobMap = scalaMap("convert-multi-csv-to-json")

    val configParams: java.util.Map[String, Any] = if (jobMap.contains("configParams"))
      jobMap("configParams").asInstanceOf[java.util.Map[String, Any]] else Map[String, Any]()
    println(configParams)
  }

  "json4s" should "json4s" in {
    import org.json4s._
    import org.json4s.native.JsonMethods._
    implicit val formats = org.json4s.DefaultFormats

    val map = parse(configStr, true).values.asInstanceOf[Map[String, Map[String, Any]]]
    println(map("convert-multi-csv-to-json")("strategy"))
  }
}
