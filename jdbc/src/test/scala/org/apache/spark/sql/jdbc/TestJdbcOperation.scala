package org.apache.spark.sql.jdbc

import java.util.Properties

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by zhengqh on 17/9/6.
  */
class TestJdbcOperation extends FlatSpec with Matchers{

  "test update" should "update" in {

    implicit def map2Prop(map: Map[String, String]): Properties = map.foldLeft(new Properties) {
      case (prop, kv) ⇒
        prop.put(kv._1, kv._2)
        prop
    }

    val sparkConf = new SparkConf().setMaster("local").setAppName("test")
    val sparkContext = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sparkContext)
    import sqlContext.implicits._

    val df = sparkContext.parallelize(Seq(
      (11, "A", 12),
      (12, "B", 12),
      (13, "C", 12)
    )).toDF("id", "name", "total")

    val jdbcSaveExplain = JdbcSaveExplain(
      "jdbc:mysql://localhost:3306/test",
      "test",
      JdbcSaveMode.Update,
      Map("user" → "root", "password" → "root")
    )
    import JdbcDataFrameWriter.dataFrame2JdbcWriter
    df.writeJdbc(jdbcSaveExplain).save()
  }
}
