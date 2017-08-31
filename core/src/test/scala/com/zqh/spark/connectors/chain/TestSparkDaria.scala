package com.zqh.spark.connectors.chain

import com.github.mrpowers.spark.daria.sql.SparkSessionExt._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
  * Created by zhengqh on 17/8/31.
  */
object TestSparkDaria {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.master("local").getOrCreate()

    val df = spark.createDF(
      List(
        ("bob", 45),
        ("liz", 25),
        ("freeman", 32)
      ), List(
        ("name", StringType, true),
        ("age", IntegerType, false)
      )
    )
  }
}
