package com.zqh.spark.connectors.config

import com.typesafe.config.Config

/**
  * Created by zhengqh on 17/9/4.
  */
object RickConfigUtil {
  implicit class RichConfig(val underlying: Config) extends AnyVal {
    def getDefaultBoolean(path: String, default: Boolean): Boolean = if (underlying.hasPath(path)) {
      underlying.getBoolean(path)
    } else {
      default
    }

    def getDefaultString(path: String, default: String): String = if (underlying.hasPath(path)) {
      underlying.getString(path)
    } else {
      default
    }

    def getDefaultInt(path: String, default: Int): Int = if (underlying.hasPath(path)) {
      underlying.getInt(path)
    } else {
      default
    }

    def getDefaultLong(path: String, default: Long): Long = if (underlying.hasPath(path)) {
      underlying.getLong(path)
    } else {
      default
    }
  }
}


