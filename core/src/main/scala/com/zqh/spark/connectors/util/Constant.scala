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
    "writer.jdbc" -> "WriteJdbc",
    "writer.codis" -> "CodisWriter"
  )

  /**
    * @param connector jdbc
    * @param rw reader
    * @return com.zqh.spark.connectors.jdbc.ReadJdbc
    */
  def getClassName(connector: String, rw: String) = packageName + connector + "." + typeClassMap(rw + "." + connector)

  /**
    * @param connectorAndMode reader.jdbc
    * @return com.zqh.spark.connectors.jdbc.ReadJdbc
    */
  def getClassName(connectorAndMode: String) = packageName + typeClassMap(connectorAndMode)

}
