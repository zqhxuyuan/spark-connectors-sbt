package com.zqh.spark.connectors.classloader

import java.io.File
import java.net.{URL, URLClassLoader}
import java.sql.SQLException

import com.zqh.spark.connectors.{NothingTransformer, SparkReader}
import com.zqh.spark.connectors.config.{ConnectorsWriteConf, ConnectorsReadConf}
import com.zqh.spark.connectors.test.{TestClassToLoader, TestSparkConnectors, TestClass2}
import junit.framework.TestCase
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import ClassLoaderUtil._
import org.junit.rules.ExpectedException
import org.junit.{Rule, Test}
import ClassConstant._

class ClassLoaderSuite extends TestCase{
  val pluginManager = new PluginManager

  def testClassLoad(): Unit = {
    val clazz = ClassLoaderUtil.loadClass(List(apiJarPath), "com.zqh.spark.connectors.test.TestClass")
    val instance = clazz.newInstance.asInstanceOf[TestClassToLoader]
    instance.method()

    // 最原始的做法
    val classLoader = new URLClassLoader(Array(new URL(jdbcJarPath)), Thread.currentThread().getContextClassLoader)
    classLoader.loadClass("com.zqh.spark.connectors.jdbc.ReadJdbc")

    // 封装的做法
    val pluginClassLoader = new PluginClassLoader(Array(new URL(jdbcJarPath)))
    pluginClassLoader.loadClass("com.zqh.spark.connectors.jdbc.ReadJdbc")

    // 更进一步的封装
    ClassLoaderUtil.loadReaderClassFromFile(List(jdbcJarPath), jdbcReadConf, "jdbc", "ReadJdbc")

    ClassLoaderUtil.loadReaderClassFromFile(List(cassJarPath), cassandraReadConf, "cassandra", "CassandraReader")
  }

  // 通过new File(filePath).toURI.toURL中的filePath不能有file:
  // 而如果是new URL(filePath), 则必须带file:. 建议使用new URL
  def testLoadOtherJarButNotExecute: Unit = {
    val jdbcReadConf = new ConnectorsReadConf("jdbc")

    // new File("/xxx").toURI.toURL
    val classLoader = new URLClassLoader(Array(new URL(jdbcJarPath)), Thread.currentThread().getContextClassLoader)
    val clazz = classLoader.loadClass(jdbcClassName)
    val reader = clazz.getConstructor(classOf[ConnectorsReadConf]).newInstance(jdbcReadConf).asInstanceOf[SparkReader]

    // new URL("file:/xxx")
    val classLoader2 = new URLClassLoader(Array(new File(jdbcJarPath.replace("file:", "")).toURI.toURL), Thread.currentThread().getContextClassLoader)
    val clazz2 = classLoader2.loadClass(jdbcClassName)
    val reader2 = clazz.getConstructor(classOf[ConnectorsReadConf]).newInstance(jdbcReadConf).asInstanceOf[SparkReader]
  }

  def testDifferentProtocols(): Unit = {
    pluginManager.loadPlugin(fileFormat(jdbcJarPath))

    pluginManager.loadPlugin(localFileFormat(jdbcJarPath))

    pluginManager.loadPlugin(jarFormat(jdbcJarPath))

    pluginManager.loadPlugin(hdfsFormat(jdbcJarPath))
  }

  def testLoadAndUnLoad(): Unit = {
    pluginManager.loadPlugin(filePluginJarPath)
    assert(pluginManager.getLoader(filePluginJarPath).cachedFileJar.size == 1)
    assert(pluginManager.getLoader(filePluginJarPath).cachedJarFiles.size == 0)

    //卸载之后, 如果找不到, 我们还是会默认返回一个PluginClassLoader
    pluginManager.unloadPlugin(filePluginJarPath)
    assert(pluginManager.getLoader(filePluginJarPath) != null)
    assert(pluginManager.getLoader(filePluginJarPath).cachedFileJar.size == 0)
    assert(pluginManager.getLoader(filePluginJarPath).cachedJarFiles.size == 0)

    val jarFile = jarFormat(jdbcJarPath)

    pluginManager.loadPlugin(jarFile)
    assert(pluginManager.getLoader(jarFile).cachedFileJar.size == 0)
    assert(pluginManager.getLoader(jarFile).cachedJarFiles.size == 1)

    pluginManager.unloadPlugin(jarFile)
    assert(pluginManager.getLoader(jarFile).cachedFileJar.size == 0)
    assert(pluginManager.getLoader(jarFile).cachedJarFiles.size == 0)
  }

  def testLoadClassAndExecute(): Unit = {
    executeLocalOrHdfsClass(pluginManager, filePluginJarPath)

    //executeLocalOrHdfsClass(hdfsPluginManager, hdfsPluginJarPath)
  }

  def executeLocalOrHdfsClass(pluginManager: IPluginManager, path: String): Unit = {
    pluginManager.loadPlugin(path)
    val classLoader = pluginManager.getLoader(path)

    assert(classLoader.isInstanceOf[ClassLoader] == true)
    assert(classLoader.isInstanceOf[URLClassLoader] == true)
    assert(classLoader.isInstanceOf[PluginClassLoader] == true)

    val clazz = Class.forName(testClassName, true, classLoader)
    val testClass = clazz.newInstance().asInstanceOf[TestClass2]
    assert(testClass.test().equals("test"))

    val conf = new SparkConf().set("write.jdbc.test", "Test")
    val config = Class.forName(writeConnectorClass, true, classLoader)
      .getConstructor(classOf[String], classOf[SparkConf])
      .newInstance("jdbc", conf).asInstanceOf[ConnectorsWriteConf]

    assert(config.getWriteConf("test").equals("Test"))

    pluginManager.unloadPlugin(path)
  }

  def testExpectedException(): Unit = {
    pluginManager.loadPlugin(jdbcJarPath)

    // Class.forName使用的是当前的ClassLoader, 因为jdbcClassName不在当前包中,所以找不到类
    try{
      Class.forName(jdbcClassName)
    } catch {
      case e: ClassNotFoundException => e.printStackTrace()
      case e: Exception => e.printStackTrace()
    }
  }

  def testLoadOtherLocalJarAndExecute: Unit = {
    pluginManager.loadPlugin(jdbcJarPath)
    val classLoader = pluginManager.getLoader(jdbcJarPath)

    // 正确的使用方式是: 使用jdbc包对应的ClassLoader,比如上面返回的classLoader
    val clazz = classLoader.loadClass(jdbcClassName)

    val reader = clazz.getConstructor(classOf[ConnectorsReadConf]).newInstance(jdbcReadConf).asInstanceOf[SparkReader]

    // 调用方法时, 会出现No suitable Driver... 这和ClassLoader没有关系,
    // 因为能够创建ReadJdbc类,说明类加载没有问题, 只不过依赖包未找到

    // 即使通过Spark的addJar仍然会报错
    spark.sparkContext.addJar(jdbcJarPath)

    try{
      reader.read(spark)
    } catch {
      case e: SQLException => e.printStackTrace
      case e: Exception => e.printStackTrace()
    }
  }

  // TODO How to resolve dependency jar, such as jdbc driver
  /*
  def testLoadHdfsJarAndExecuteJob(): Unit = {
    val jdbcJar = hdfsFormat("/user/pontus/jdbc-assembly-0.0.1.jar")
    spark.sparkContext.addJar(jdbcJar)

    pluginManager.loadPlugin(jdbcJar)
    val classLoader = pluginManager.getLoader(jdbcJar)

    val clazz = classLoader.loadClass(jdbcClassName)
    val reader = clazz.getConstructor(classOf[ConnectorsReadConf]).newInstance(jdbcReadConf).asInstanceOf[SparkReader]

    try {
      reader.read(spark)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  def testHdfsJar(): Unit = {
    val jdbcJar = hdfsFormat("/user/pontus/jdbc-assembly-0.0.1.jar")
    val reader = ClassLoaderUtil.loadReaderClassFromFile(List(jdbcJar), jdbcReadConf, "jdbc", "ReadJdbc")
    reader.read(spark)
  }

  def testCodis() = {
    pluginManager.loadPlugin(apiJarPath)
    pluginManager.loadPlugin(codisJarPath)

    val reader = ClassLoaderUtil.loadReaderClassFromFile(List(codisJarPath), testCodisReadConf, "", "com.zqh.spark.connectors.test.TestCodisReader")
    val writer = ClassLoaderUtil.loadWriterClassFromFile(List(codisJarPath), codisWriteConf, "codis", "CodisWriter")
    val transformer = new NothingTransformer
    val connector = new TestSparkConnectors(reader, writer, transformer, spark)
    connector.runSparkJob()
  }
  */
}
