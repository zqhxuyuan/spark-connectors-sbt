package org.apache.spark.sql.jdbc

import java.util.Properties

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.jdbc.JdbcSaveMode.{SaveMode, _}

class JdbcDataFrameWriter(dataFrame: DataFrame) extends Serializable with Logging {
  private var jdbcSaveExplain: JdbcSaveExplain = _
  private val extraOptions = new scala.collection.mutable.HashMap[String, String]

  def writeJdbc(jdbcSaveExplain: JdbcSaveExplain) = {
    this.jdbcSaveExplain = jdbcSaveExplain
    this
  }

  def save(): Unit = {
    assert(jdbcSaveExplain != null)
    val saveMode = jdbcSaveExplain.saveMode
    val url = jdbcSaveExplain.url
    val table = jdbcSaveExplain.tableName
    val props = jdbcSaveExplain.jdbcParam
    if (checkTable(url, table, props, saveMode))
      JdbcExtendUtil.saveTable(dataFrame, url, table, props, saveMode)
  }

  private def checkTable(url: String, table: String, connectionProperties: Properties, saveMode: SaveMode): Boolean = {
    val props = new Properties()
    extraOptions.foreach { case (key, value) =>
      props.put(key, value)
    }
    // connectionProperties should override settings in extraOptions
    props.putAll(connectionProperties)
    val conn = JdbcExtendUtil.createConnectionFactory(url, props)()

    try {
      var tableExists = JdbcExtendUtil.tableExists(conn, url, table)
      //table ignore ,exit
      if (saveMode == IgnoreTable && tableExists) {
        log.info("table {} exists ,mode is ignoreTable,save nothing to it", table)
        return false
      }
      //error if table exists
      if (saveMode == ErrorIfExists && tableExists) {
        sys.error(s"Table $table already exists.")
      }
      //overwrite table ,delete table
      if (saveMode == Overwrite && tableExists) {
        JdbcExtendUtil.dropTable(conn, table)
        tableExists = false
      }
      // Create the table if the table didn't exist.
      if (!tableExists) {
        checkField(dataFrame)
        val schema = JdbcExtendUtil.schemaString(dataFrame, url)
        val sql = s"CREATE TABLE $table (id int not null primary key auto_increment , $schema)"
        conn.prepareStatement(sql).executeUpdate()
      }
      true
    } finally {
      conn.close()
    }
  }

  //because table in mysql need id  as primary key auto increment,illegal if dataFrame contains id  field
  private def checkField(dataFrame: DataFrame): Unit = {
    if (dataFrame.schema.exists(_.name == "id")) {
      throw new IllegalArgumentException("dataFrame exists id columns,but id is primary key auto increment in mysql ")
    }
  }
}

object JdbcDataFrameWriter {
  implicit def dataFrame2JdbcWriter(df: DataFrame): JdbcDataFrameWriter = JdbcDataFrameWriter(df)

  def apply(df: DataFrame): JdbcDataFrameWriter = new JdbcDataFrameWriter(df)
}
