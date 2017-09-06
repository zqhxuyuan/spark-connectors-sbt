package org.apache.spark.sql.jdbc

import java.util.Properties

import org.apache.spark.sql.jdbc.JdbcSaveMode.SaveMode

case class JdbcSaveExplain(url: String, tableName: String, saveMode: SaveMode, jdbcParam: Properties)