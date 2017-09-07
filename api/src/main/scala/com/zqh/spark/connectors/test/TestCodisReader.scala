package com.zqh.spark.connectors.test

import com.zqh.spark.connectors.{ConnectorsReadConf, SparkReader}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by zhengqh on 17/9/7.
  */
class TestCodisReader(conf: ConnectorsReadConf) extends SparkReader{

  override def read(spark: SparkSession): DataFrame = {
    val operator = conf.getReadConf("op", "set")
    val sc = spark.sparkContext
    import spark.implicits._

    val df = operator match {
      case "set" => // set key1 value1
        sc.parallelize(List(
          ("key1", "value1"),
          ("key2", "value2")
        )).toDF()
      case "hset" => // hset hashset1 key1 value1
        sc.parallelize(List(
          ("hashset1", "key1", "value1"),
          ("hashset1", "key1", "value1"),
          ("hashset2", "key1", "value1"),
          ("hashset2", "key2", "value2")
        )).toDF()
      case "lpush" => // lpush list1 value1
        sc.parallelize(List(
          ("list1", "value1"),
          ("list1", "value2"),
          ("list2", "value1"),
          ("list2", "value2")
        )).toDF()
      case "sadd" => // sadd set1 value1
        sc.parallelize(List(
          ("set1", "value1"),
          ("set1", "value2"),
          ("set2", "value2"),
          ("set2", "value2")
        )).toDF()
      case "zadd" => // zadd zset1 11111 member1
        sc.parallelize(List(
          ("zset1", "11111", "member1"),
          ("zset1", "11112", "member2"),
          ("zset1", "11110", "member1"),  // filter demo
          ("zset1", "11113", "member2"),  // filter demo
          ("zset2", "11111", "member1"),
          ("zset2", "11112", "member2")
        )).toDF()
    }
    df.show
    df
  }
}
