package com.zqh.spark.connectors.classloader

import java.io.File

import com.zqh.spark.connectors.{SparkWriter, ConnectorsWriteConf, ConnectorsReadConf, SparkReader}

/**
  * Created by zhengqh on 17/8/31.
  */
object ClassLoaderUtil {

  def loadReaderClass(jarPath: List[String],
                      conf: ConnectorsReadConf,
                      connector: String,
                      className: String
                     ): SparkReader = {

    val packagePrefix = "com.zqh.spark.connectors."
    val fullClassName = packagePrefix + s"$connector.$className"

    val urls = jarPath.map(file => new File(file).toURI.toURL)
    var classLoader = new java.net.URLClassLoader(urls.toArray, this.getClass.getClassLoader)
    val clazz = classLoader.loadClass(fullClassName)

    clazz.getConstructor(classOf[ConnectorsReadConf]).
      newInstance(conf).asInstanceOf[SparkReader]
  }

  def loadWriterClass(jarPath: List[String],
                      conf: ConnectorsWriteConf,
                      connector: String,
                      className: String
                     ): SparkWriter = {

    val packagePrefix = "com.zqh.spark.connectors."
    val fullClassName = packagePrefix + s"$connector.$className"

    val urls = jarPath.map(file => new File(file).toURI.toURL)
    var classLoader = new java.net.URLClassLoader(urls.toArray, this.getClass.getClassLoader)
    val clazz = classLoader.loadClass(fullClassName)

    clazz.getConstructor(classOf[ConnectorsWriteConf]).
      newInstance(conf).asInstanceOf[SparkWriter]
  }

  def addJarToClassPath(jarPath: List[String]) = {
    val urls = jarPath.map(file => new File(file).toURI.toURL)
    var classLoader = new java.net.URLClassLoader(urls.toArray, this.getClass.getClassLoader)
    Thread.currentThread().setContextClassLoader(classLoader);
  }
}
