package com.zqh.spark.connectors

/**
  * Created by zhengqh on 17/9/8.
  */
object ConnectorParameters {

  object Codis {
    val codisZkHost = "zkHost"
    val codisZkTimeout = "zkTimeout"
    val codisZkDir = "zkDir"
    val codisPassword = "password"
    val codisFilter = "filter"
    val codisTTL = "ttl"
    val codisPrefix = "prefix"
    val codisCommand = "command"
  }

  object Jdbc {

  }

  object Cassandra {

  }

  object redis {

  }

  object Ftp {

  }
}
