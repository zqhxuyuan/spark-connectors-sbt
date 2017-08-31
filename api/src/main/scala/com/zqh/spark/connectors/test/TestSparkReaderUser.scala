package com.zqh.spark.connectors.test

import com.zqh.spark.connectors.SparkReader
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by zhengqh on 17/8/29.
  */
class TestSparkReaderUser extends SparkReader{

  override def read(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val ds = spark.sparkContext.parallelize(
      List(
        (1, "z", "qh"),
        (2, "x", "yy"),
        (3, "t", "bag")
      )
    ).toDF("user_id", "fname", "lname")
    ds
  }
}
