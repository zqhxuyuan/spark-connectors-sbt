package com.zqh.spark.connectors.core

import java.util.concurrent.Executors

import com.zqh.spark.connectors.{SparkReader, SparkWriter}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.util.{Success, Failure}

/**
  * Created by zhengqh on 17/8/30.
  */
class SparkParallelPipeline(readers: List[SparkReader], writers: List[SparkWriter], spark: SparkSession) {

    def runSparkJob(): Unit = {
      val pool = Executors.newFixedThreadPool(5)
      implicit val xc = ExecutionContext.fromExecutorService(pool)

      // 初始化
      readers.foreach(reader => reader.init(spark))

      // 读取源
      val union: DataFrame = null

      val readFutures: List[Future[DataFrame]] = readers.map(reader => {
        Future {
          reader.read(spark)
        }
      })
      val sequenceFutures: Future[List[DataFrame]] = Future.sequence(readFutures)

      sequenceFutures.onComplete({
        case Success(dfs) =>
          dfs.foreach(df => union.union(df))
        case Failure(e) =>
      })

      println("finish reading all data source.")

      // 写入目标
      val writeFutures = writers.map(writer => {
        Future {
          writer.write(union)
          writer.close()
        }
      })
      Await.result(Future.sequence(writeFutures), Duration.Inf)

      // 关闭
      spark.close()
    }
  }
