package org.apache.spark.sql.jdbc

import java.sql.{Connection, Driver, DriverManager, PreparedStatement, SQLException}
import java.util.Properties

import org.apache.spark.sql._
import org.apache.spark.sql.execution.datasources.jdbc.{JdbcUtils, DriverRegistry, DriverWrapper}
import org.apache.spark.sql.jdbc.JdbcSaveMode.{SaveMode, _}
import org.apache.spark.sql.types.{LongType, StructType, _}

import scala.collection.JavaConverters._
import scala.util.Try

object JdbcExtendUtil{

  // the property names are case sensitive
  val JDBC_BATCH_FETCH_SIZE = "fetchsize"
  val JDBC_BATCH_INSERT_SIZE = "batchsize"

  def saveTable(df: DataFrame,
                url: String,
                table: String,
                properties: Properties,
                saveMode: SaveMode ) {
    val dialect = JdbcDialects.get(url)
    val nullTypes: Array[Int] = df.schema.fields.map { field =>
      getJdbcType(field.dataType, dialect).jdbcNullType
    }

    val rddSchema = df.schema
    val getConnection: () => Connection = createConnectionFactory(url, properties)
    val batchSize = properties.getProperty(JDBC_BATCH_INSERT_SIZE, "1000").toInt
    df.foreachPartition { iterator =>
      savePartition(getConnection, table, iterator, rddSchema, nullTypes, batchSize, dialect,saveMode)
    }
  }

  def insertStatement(conn: Connection, table: String, rddSchema: StructType, dialect: JdbcDialect, saveMode: SaveMode)
  : PreparedStatement = {
    val columnNames = rddSchema.fields.map(x => dialect.quoteIdentifier(x.name))
    val columns = columnNames.mkString(",")
    val placeholders = rddSchema.fields.map(_ => "?").mkString(",")

    val sql = saveMode match {
      case Update =>
        val duplicateSetting = columnNames.map(name ⇒ s"$name=?").mkString(",")
        s"INSERT INTO $table ($columns) VALUES ($placeholders) ON DUPLICATE KEY UPDATE $duplicateSetting"
      case Append | Overwrite =>
        s"INSERT INTO $table ($columns) VALUES ($placeholders)"
      case IgnoreRecord =>
        s"INSERT IGNORE INTO $table ($columns) VALUES ($placeholders)"
      case _ ⇒ throw new IllegalArgumentException(s"$saveMode is illegal")
    }
    conn.prepareStatement(sql)
  }

  def savePartition(
                     getConnection: () => Connection,
                     table: String,
                     iterator: Iterator[Row],
                     rddSchema: StructType,
                     nullTypes: Array[Int],
                     batchSize: Int,
                     dialect: JdbcDialect,
                     saveMode: SaveMode) = {
    require(batchSize >= 1, s"Invalid value `${batchSize.toString}` for parameter " +
      s"`$JDBC_BATCH_INSERT_SIZE`. The minimum value is 1.")

    val conn = getConnection()
    conn.setAutoCommit(false) // Everything in the same db transaction.
    conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED)

    var committed = false
    try {
      val isUpdateMode = saveMode == Update //check is UpdateMode
      val length = rddSchema.fields.length
      val numFields = if (isUpdateMode) length * 2 else length // real num Field length
      val stmt = insertStatement(conn, table, rddSchema, dialect, saveMode)
      val setters: Array[JDBCValueSetter] = getSetter(rddSchema.fields, conn, dialect, isUpdateMode) //call method getSetter
      var rowCount = 0
      while (iterator.hasNext) {
        val row = iterator.next()
        var i = 0
        val midField = numFields / 2
        while (i < numFields) {
          //if duplicate ,'?' size = 2 * row.field.length
          if (isUpdateMode) {
            i < midField match {
              // check midField > i ,if midFiled >i ,rowIndex is setterIndex - (setterIndex/2) + 1
              case true ⇒
                if (row.isNullAt(i)) {
                  stmt.setNull(i + 1, nullTypes(i))
                } else {
                  setters(i).apply(stmt, row, i, 0)
                }
              case false ⇒
                if (row.isNullAt(i - midField)) {
                  stmt.setNull(i + 1, nullTypes(i - midField))
                } else {
                  setters(i).apply(stmt, row, i, midField)
                }
            }
          } else {
            if (row.isNullAt(i)) {
              stmt.setNull(i + 1, nullTypes(i))
            } else {
              setters(i).apply(stmt, row, i, 0)
            }
          }
          i = i + 1
        }
        stmt.addBatch()
        rowCount += 1
        if (rowCount % batchSize == 0) {
          stmt.executeBatch()
          rowCount = 0
        }

      }
      if (rowCount > 0) {
        stmt.executeBatch()
      }
      stmt.close()
      conn.commit()
      committed = true
    } catch {
      case e: SQLException =>
        val cause = e.getNextException
        if (e.getCause != cause) {
          if (e.getCause == null) {
            e.initCause(cause)
          } else {
            e.addSuppressed(cause)
          }
        }
        throw e
    } finally {
      if (!committed) {
        // The stage must fail.  We got here through an exception path, so
        // let the exception through unless rollback() or close() want to
        // tell the user about another problem.
        conn.rollback()
        conn.close()
      } else {
        // The stage must succeed.  We cannot propagate any exception close() might throw.
        try {
          conn.close()
        } catch {
          case e: Exception => println("Transaction succeeded, but closing failed", e)
        }
      }
    }

  }

  type JDBCValueSetter = (PreparedStatement, Row, Int, Int) => Unit

  def makeSetter(
                  conn: Connection,
                  dialect: JdbcDialect,
                  dataType: DataType): JDBCValueSetter = {
    dataType match {
      case IntegerType ⇒
        (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) ⇒
          stmt.setInt(pos + 1, row.getInt(pos - offset))

      case LongType ⇒
        (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) ⇒
          stmt.setLong(pos + 1, row.getLong(pos - offset))

      case DoubleType ⇒
        (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) ⇒
          stmt.setDouble(pos + 1, row.getDouble(pos - offset))

      case FloatType ⇒
        (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) ⇒
          stmt.setFloat(pos + 1, row.getFloat(pos - offset))

      case ShortType ⇒
        (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) ⇒
          stmt.setInt(pos + 1, row.getShort(pos - offset))

      case ByteType ⇒
        (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) ⇒
          stmt.setInt(pos + 1, row.getByte(pos - offset))

      case BooleanType ⇒
        (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) ⇒
          stmt.setBoolean(pos + 1, row.getBoolean(pos - offset))

      case StringType ⇒
        (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) ⇒
          stmt.setString(pos + 1, row.getString(pos - offset))

      case BinaryType ⇒
        (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) ⇒
          stmt.setBytes(pos + 1, row.getAs[Array[Byte]](pos - offset))

      case TimestampType ⇒
        (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) ⇒
          stmt.setTimestamp(pos + 1, row.getAs[java.sql.Timestamp](pos - offset))

      case DateType ⇒
        (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) ⇒
          stmt.setDate(pos + 1, row.getAs[java.sql.Date](pos - offset))

      case t: DecimalType ⇒
        (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) ⇒
          stmt.setBigDecimal(pos + 1, row.getDecimal(pos - offset))

      case ArrayType(et, _) ⇒
        // remove type length parameters from end of type name
        val typeName = getJdbcType(et, dialect).databaseTypeDefinition
          .toLowerCase.split("\\(")(0)
        (stmt: PreparedStatement, row: Row, pos: Int, offset: Int) ⇒
          val array = conn.createArrayOf(
            typeName,
            row.getSeq[AnyRef](pos - offset).toArray)
          stmt.setArray(pos + 1, array)

      case _ ⇒
        (_: PreparedStatement, _: Row, pos: Int, offset: Int) ⇒
          throw new IllegalArgumentException(
            s"Can't translate non-null value for field $pos")
    }
  }

  def getSetter(fields: Array[StructField], connection: Connection, dialect: JdbcDialect, isUpdateMode: Boolean): Array[JDBCValueSetter] = {
    val setter = fields.map(_.dataType).map(makeSetter(connection, dialect, _))
    if (isUpdateMode) {
      Array.fill(2)(setter).flatten
    } else {
      setter
    }
  }

  def getJdbcType(dt: DataType, dialect: JdbcDialect): JdbcType = {
    dialect.getJDBCType(dt).orElse(JdbcUtils.getCommonJDBCType(dt)).getOrElse(
      throw new IllegalArgumentException(s"Can't get JDBC type for ${dt.simpleString}"))
  }

  /**
    * Returns a factory for creating connections to the given JDBC URL.
    *
    * @param url the JDBC url to connect to.
    * @param properties JDBC connection properties.
    */
  def createConnectionFactory(url: String, properties: Properties): () => Connection = {
    val userSpecifiedDriverClass = Option(properties.getProperty("driver"))
    userSpecifiedDriverClass.foreach(DriverRegistry.register)
    // Performing this part of the logic on the driver guards against the corner-case where the
    // driver returned for a URL is different on the driver and executors due to classpath differences.
    val driverClass: String = userSpecifiedDriverClass.getOrElse {
      DriverManager.getDriver(url).getClass.getCanonicalName
    }
    () => {
      DriverRegistry.register(driverClass)
      val driver: Driver = DriverManager.getDrivers.asScala.collectFirst {
        case d: DriverWrapper if d.wrapped.getClass.getCanonicalName == driverClass => d
        case d if d.getClass.getCanonicalName == driverClass => d
      }.getOrElse {
        throw new IllegalStateException(
          s"Did not find registered driver with class $driverClass")
      }
      driver.connect(url, properties)
    }
  }

  /*
  * Returns true if the table already exists in the JDBC database.
  */
  def tableExists(conn: Connection, url: String, table: String): Boolean = {
    val dialect = JdbcDialects.get(url)
    // Somewhat hacky, but there isn't a good way to identify whether a table exists for all
    // SQL database systems using JDBC meta data calls, considering "table" could also include
    // the database name. Query used to find table exists can be overridden by the dialects.
    Try {
      val statement = conn.prepareStatement(dialect.getTableExistsQuery(table))
      try {
        statement.executeQuery()
      } finally {
        statement.close()
      }
    }.isSuccess
  }

  /**
    * Drops a table from the JDBC database.
    */
  def dropTable(conn: Connection, table: String): Unit = {
    val statement = conn.createStatement
    try {
      statement.executeUpdate(s"DROP TABLE $table")
    } finally {
      statement.close()
    }
  }

  /**
    * Compute the schema string for this RDD.
    */
  def schemaString(df: DataFrame, url: String): String = {
    val sb = new StringBuilder()
    val dialect = JdbcDialects.get(url)
    df.schema.fields foreach { field =>
      val name = dialect.quoteIdentifier(field.name)
      val typ: String = getJdbcType(field.dataType, dialect).databaseTypeDefinition
      val nullable = if (field.nullable) "" else "NOT NULL"
      sb.append(s", $name $typ $nullable")
    }
    if (sb.length < 2) "" else sb.substring(2)
  }
}
