package com.zqh.spark.connectors

import com.zqh.spark.connectors.config.ConfigUtils
import com.zqh.spark.connectors.core.SparkPipelines
import com.zqh.spark.connectors.util.Constant
import org.apache.spark.sql.SparkSession

/**
  * Created by zhengqh on 17/8/31.
  *
  bin/spark-submit --master local \
  --driver-class-path /Users/zhengqh/.m2/repository/mysql/mysql-connector-java/5.1.40/mysql-connector-java-5.1.40.jar \
  --jars /Users/zhengqh/spark-connectors-sbt/jdbc/target/scala-2.11/jdbc-assembly-0.0.1.jar \
  --class com.zqh.spark.connectors.ConnectorClient \
  --files /Users/zhengqh/spark-connectors-sbt/core/src/main/resources/application.conf \
  --driver-java-options -Dconfig.file=/Users/zhengqh/spark-connectors-sbt/core/src/main/resources/application.conf \
  /Users/zhengqh/spark-connectors-sbt/core/target/scala-2.11/core-assembly-0.0.1.jar
  */
object ConnectorClient {

  def main(args: Array[String]) {
    val connectors = ConfigUtils.loadConfig()
    val readerConfigs = connectors("readers")
    val writerConfigs = connectors("writers")

    val readerConnectors = readerConfigs.map(reader => {
      val readerType = reader(Constant.FORMAT)
      println("reader: " + readerType)
      val conf = new ConnectorsReadConf(readerType)
      reader.filterKeys(!_.equals(Constant.FORMAT)).foreach(kv => {
        println(kv._1 + ": " + kv._2)
        conf.setReadConf(kv._1, kv._2)
      })

      val clazz = Class.forName(Constant.getClassName(readerType, Constant.READER)).
        getConstructor(classOf[ConnectorsReadConf]).newInstance(conf).asInstanceOf[SparkReader]
      clazz
    })

    val writerConnectors = writerConfigs.map(writer => {
      val writerType = writer(Constant.FORMAT)
      println("writer: " + writerType)
      val conf = new ConnectorsWriteConf(writerType)
      writer.filterKeys(!_.equals(Constant.FORMAT)).foreach(kv => {
        println(kv._1 + ": " + kv._2)
        conf.setWriteConf(kv._1, kv._2)
      })

      val clazz = Class.forName(Constant.getClassName(writerType, Constant.WRITER)).
        getConstructor(classOf[ConnectorsWriteConf]).newInstance(conf).asInstanceOf[SparkWriter]
      clazz
    })

    val spark = SparkSession.builder().getOrCreate()

    val job = new SparkPipelines(readerConnectors, writerConnectors, spark)
    job.runSparkJob()
  }
}
