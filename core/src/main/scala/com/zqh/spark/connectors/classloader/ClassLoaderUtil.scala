package com.zqh.spark.connectors.classloader

import java.net.{URL, URLClassLoader}

import com.zqh.spark.connectors.{SparkWriter, ConnectorsWriteConf, ConnectorsReadConf, SparkReader}
import org.apache.hadoop.conf.Configuration

/**
  * Created by zhengqh on 17/8/31.
  */
object ClassLoaderUtil {

  val packagePrefix = "com.zqh.spark.connectors."

  def hdfsSchema = new Configuration().get("fs.defaultFS") // hdfs://tdhdfs

  def fileFormat(path: String) = "file:" + path.replace("file:", "")
  def jarFormat(path: String) = "jar:file:" + path.replace("file:", "") + "!/"
  def localFileFormat(path: String) = path.replace("file:", "")
  def hdfsFormat(path: String) = hdfsSchema + path.replace("file:", "")

  def filesToURL(jarPath: List[String]) = jarPath.map(file => new URL(file))

  /**
    * 创建URLClassLoader有多种方式
    *
    * URLClassLoader.newInstance(urls.toArray, this.getClass.getClassLoader)
    * new URLClassLoader(urls.toArray, this.getClass.getClassLoader)
    * new PluginClassLoader(urls.toArray)
    *
    * @param jarPath 必须包含protocol, 比如本地文件是file://,HDFS是hdfs://
    */
  def newClassLoader(jarPath: List[String]) = {
    new URLClassLoader(filesToURL(jarPath).toArray, Thread.currentThread().getContextClassLoader)
  }

  def addJarToClassPath(jarPath: List[String]): ClassLoader = {
    val classLoader = newClassLoader(jarPath)
    Thread.currentThread().setContextClassLoader(classLoader)
    Thread.currentThread().getContextClassLoader
  }

  def loadClass(jarPath: List[String], className: String): Class[_] = {
    val classLoader = newClassLoader(jarPath)
    classLoader.loadClass(className)
  }

  def loadReaderClassFromFile(jarPath: List[String],
                              conf: ConnectorsReadConf,
                              connector: String,
                              className: String
                     ): SparkReader = {
    val classLoader = newClassLoader(jarPath)
    loadReaderClassFromClassLoader(classLoader, conf, connector, className)
  }

  def loadWriterClassFromFile(jarPath: List[String],
                              conf: ConnectorsWriteConf,
                              connector: String,
                              className: String
                     ): SparkWriter = {
    val classLoader = newClassLoader(jarPath)
    loadWriterClassFromClassLoader(classLoader, conf, connector, className)
  }

  def loadReaderClassFromClassLoader(classLoader: ClassLoader,
                                     conf: ConnectorsReadConf,
                                     connector: String,
                                     className: String
                                    ): SparkReader = {
    val fullClassName = packagePrefix + s"$connector.$className"
    val clazz = classLoader.loadClass(fullClassName)
    clazz.getConstructor(classOf[ConnectorsReadConf]).
      newInstance(conf).asInstanceOf[SparkReader]
  }

  def loadWriterClassFromClassLoader(classLoader: ClassLoader,
                                     conf: ConnectorsWriteConf,
                                     connector: String,
                                     className: String
                                    ): SparkWriter = {
    val fullClassName = packagePrefix + s"$connector.$className"
    val clazz = classLoader.loadClass(fullClassName)

    clazz.getConstructor(classOf[ConnectorsWriteConf]).
      newInstance(conf).asInstanceOf[SparkWriter]
  }
}
