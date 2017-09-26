package com.zqh.spark.connectors.socket

import junit.framework.TestCase
import org.apache.spark.sql.types.StructType

/**
  * Created by zhengqh on 17/9/8.
  */
class TestStructureStreaming extends TestCase{

  import org.apache.spark.sql.functions._
  import org.apache.spark.sql.SparkSession

  val spark = SparkSession
    .builder
    .appName("StructuredNetworkWordCount")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  def testSocketStreaming(): Unit = {
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()

    // Start running the query that prints the running counts to the console
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()
  }

  def testReadCsv(): Unit = {
    // Read all the csv files written atomically in a directory
    val userSchema = new StructType().add("name", "string").add("age", "integer")
    val csvDF = spark
      .readStream
      .option("sep", ",")
      .schema(userSchema)
      .format("csv")
      .load("/path/to/directory")
  }
}
