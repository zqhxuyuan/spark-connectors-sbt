package org.apache.spark.sql.jdbc

/**
  * Created by zhengqh on 17/9/6.
  */
object JdbcSaveMode extends Enumeration {
  type SaveMode = Value
  val IgnoreTable, Append, Overwrite, Update, ErrorIfExists, IgnoreRecord = Value
}
