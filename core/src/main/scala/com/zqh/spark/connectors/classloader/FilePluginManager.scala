package com.zqh.spark.connectors.classloader

import java.net.{MalformedURLException, URL}
import scala.collection.mutable

/**
  * Created by zhengqh on 17/9/6.
  */
class FilePluginManager extends IPluginManager{

  def loadPlugin(pluginName: String): PluginClassLoader = {
    pluginMap -= pluginName
    val loader = new PluginClassLoader
    var url: URL = null
    try {
      url = new URL(pluginName)
      loader.addURLFile(url)
      addLoader(pluginName, loader)
      System.out.println("load " + pluginName + "  success")
    } catch {
      case e: MalformedURLException => e.printStackTrace
    }
    loader
  }
}

