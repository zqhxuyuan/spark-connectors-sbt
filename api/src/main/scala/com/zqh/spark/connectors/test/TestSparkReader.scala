package com.zqh.spark.connectors.test

import com.zqh.spark.connectors.SparkReader
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by zhengqh on 17/8/29.
  */
class TestSparkReader extends SparkReader{

  override def read(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val ds = spark.sparkContext.parallelize(List(1,2,3)).toDF()
    ds
  }

}
