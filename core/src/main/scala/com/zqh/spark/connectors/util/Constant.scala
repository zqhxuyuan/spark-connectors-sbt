package com.zqh.spark.connectors.util

/**
  * Created by zhengqh on 17/8/31.
  */
object Constant {

  final val packageName = "com.zqh.spark.connectors."
  final val READER = "reader"
  final val WRITER = "writer"
  final val FORMAT = "format"

  final val DATAFRAME_READER = "org.zqh.spark.connectors.dataframe.DFReader"
  final val DATAFRAME_WRITER = "org.zqh.spark.connectors.dataframe.DFWriter"

  val classNameMapping = Map[String, String](
    "reader.codis" -> "org.zqh.spark.connectors.dataframe.DFReader"
  )

  def getClassName(rw: String, connector: String) =
    classNameMapping.get(rw + "." + connector) match {
      case Some(className) => className
      case None =>
        rw match {
          case READER => DATAFRAME_READER
          case WRITER => DATAFRAME_WRITER
        }
    }
}
