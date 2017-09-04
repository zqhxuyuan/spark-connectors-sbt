package com.zqh.spark.connectors.util

/**
  * Created by zhengqh on 17/8/31.
  */
object Constant {

  final val packageName = "com.zqh.spark.connectors."
  final val READER = "reader"
  final val WRITER = "writer"
  final val FORMAT = "format"

  val typeClassMap = Map(
    "reader.jdbc" -> "ReadJdbc",
    "writer.jdbc" -> "WriteJdbc"
  )

  /**
    * com.zqh.spark.connectors.jdbc.ReadJdbc
    * @param connector jdbc
    * @param rw reader
    * @return
    */
  def getClassName(connector: String, rw: String) = packageName + connector + "." + typeClassMap(rw + "." + connector)

}
