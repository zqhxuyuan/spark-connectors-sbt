package com.zqh.spark.connectors.test

import com.zqh.spark.connectors.SparkReader
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by zhengqh on 17/8/29.
  */
class TestSparkReader2 extends SparkReader{

  override def read(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val ds = spark.sparkContext.parallelize(
      List(
        (1, "A", 1),
        (2, "B", 2),
        (3, "C", 3)
      )
    ).toDF("id", "name", "total")
    ds
  }
}
