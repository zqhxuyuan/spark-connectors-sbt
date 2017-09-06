package com.zqh.spark.connectors.classloader

import java.net.{MalformedURLException, URL}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FsUrlStreamHandlerFactory, Path, FileSystem}

import scala.collection.mutable

/**
  * Created by zhengqh on 17/9/6.
  */
class HdfsPluginManager extends IPluginManager{

  def loadPlugin(pluginName: String) = {
    pluginMap -= pluginName
    val loader = new PluginClassLoader
    var url: URL = null
    try {
      // HDFS BEGIN
      val fsUrlStreamHandlerFactory = new FsUrlStreamHandlerFactory()
      URL.setURLStreamHandlerFactory(fsUrlStreamHandlerFactory)

      var conf = new Configuration
      //conf.addResource("core-site.xml")
      //conf.addResource("hdfs-site.xml")

      val fileSystem = FileSystem.get(conf)
      val path = new Path(pluginName);
      if (!fileSystem.exists(path)) {
        println("File does not exists")
      }
      val uriPath = path.toUri()
      val urlPath = uriPath.toURL()
      // HDFS END

      loader.addURLFile(urlPath)
      addLoader(pluginName, loader)
      System.out.println("load " + pluginName + "  success")
    } catch {
      case e: MalformedURLException => e.printStackTrace
    }
    loader
  }
}