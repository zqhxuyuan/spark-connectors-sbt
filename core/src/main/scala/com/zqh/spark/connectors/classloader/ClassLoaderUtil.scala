package com.zqh.spark.connectors.classloader

import java.io.File
import java.net.URLClassLoader

import com.zqh.spark.connectors.{SparkWriter, ConnectorsWriteConf, ConnectorsReadConf, SparkReader}
import org.apache.spark.SparkConf

/**
  * Created by zhengqh on 17/8/31.
  */
object ClassLoaderUtil {

  val packagePrefix = "com.zqh.spark.connectors."

  def filesToURL(jarPath: List[String]) = jarPath.map(file => new File(file).toURI.toURL)

  def filesToClassLoader(jarPath: List[String]) = {
    val urls = filesToURL(jarPath)
    //val classloader = URLClassLoader.newInstance(urls.toArray, this.getClass.getClassLoader)
    //val classloader = new URLClassLoader(urls.toArray, this.getClass.getClassLoader)
    val classloader = new URLClassLoader(urls.toArray, Thread.currentThread().getContextClassLoader)
    //val classloader = new PluginClassLoader(urls.toArray)
    classloader
  }

  def loadReaderClassFromFile(jarPath: List[String],
                              conf: ConnectorsReadConf,
                              connector: String,
                              className: String
                     ): SparkReader = {
    val classLoader = filesToClassLoader(jarPath)
    loadReaderClassFromClassLoader(classLoader, conf, connector, className)
  }

  def loadWriterClassFromFile(jarPath: List[String],
                              conf: ConnectorsWriteConf,
                              connector: String,
                              className: String
                     ): SparkWriter = {
    val classLoader = filesToClassLoader(jarPath)
    loadWriterClassFromClassLoader(classLoader, conf, connector, className)
  }

  def addJarToClassPath(jarPath: List[String]) = {
    val classLoader = filesToClassLoader(jarPath)
    Thread.currentThread().setContextClassLoader(classLoader)
    Thread.currentThread().getContextClassLoader
  }

  def loadClassFromJar(jarPath: List[String], className: String): Class[_] = {
    val classLoader = filesToClassLoader(jarPath)
    val clazz = classLoader.loadClass(className)
    clazz
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
