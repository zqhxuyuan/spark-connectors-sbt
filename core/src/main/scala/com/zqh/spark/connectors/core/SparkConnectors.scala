package com.zqh.spark.connectors.core

import com.zqh.spark.connectors.{SparkReader, SparkWriter}
import org.apache.spark.sql.SparkSession

/**
  * Created by zhengqh on 17/8/29.
  */
class SparkConnectors(reader: SparkReader,
                      writer: SparkWriter,
                      spark: SparkSession) {
  def runSparkJob(): Unit = {
    // 初始化
    reader.init(spark)

    // 读取源
    val source = reader.read(spark)

    //val transform = transformer.transform(source)

    // 写入目标
    writer.write(source)

    // 关闭
    writer.close()

    spark.close()
  }
}
