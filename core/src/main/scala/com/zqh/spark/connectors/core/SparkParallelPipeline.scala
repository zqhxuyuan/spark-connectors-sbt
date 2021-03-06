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
      val readFutures: List[Future[DataFrame]] = readers.map(reader => {
        Future {
          reader.read(spark)
        }
      })
      val sequenceFutures: Future[List[DataFrame]] = Future.sequence(readFutures)

      var union: DataFrame = null
      def readResultFuture = sequenceFutures.map(dfs => {
        dfs.foreach(df => {
          if(union == null) union = df
          else union = union.union(df)
        })
        union
      })

      def writeFutures(unionDF: DataFrame) = writers.map(writer => {
        Future {
          writer.write(unionDF)
          writer.close()
        }
      })

      readResultFuture.andThen {
        case Success(df) =>
          println("finish reading all data source.")
          // 写入目标
          val sequenceWriteFutures = Future.sequence(writeFutures(df))
          sequenceWriteFutures.onComplete{
            case Success(_) =>
              println("finish writing all data sink.")
              spark.close
              System.exit(0)
            case Failure(e) => e.printStackTrace()
          }
        case Failure(e) => e.printStackTrace()
      }
    }
  }
