package com.zqh.spark.connectors.test

import com.zqh.spark.connectors.SparkWriter
import org.apache.spark.sql.DataFrame

/**
  * Created by zhengqh on 17/8/29.
  */
class ConsoleSparkWriter extends SparkWriter{

  override def write(df: DataFrame) = {
    df.take(100).foreach(println)
  }
}
