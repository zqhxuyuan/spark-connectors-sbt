
# spark connectors with sbt

- [x] jdbc
- [x] cassandra
- [x] mongodb
- [ ] codis/redis
- [ ] hive
- [ ] hdfs/file
- [ ] hbase
- [ ] elasticsearch

# Architecture

ConnectorClient is the entry point of spark connectors.

# How to extend plugin

1. reader extends SparkReader by pass ConnectorsReadConf.
2. implements read method and return DataFrame.
3. writer is the same as reader, That's all.

check jdbc plugin for example:

```
class ReadJdbc(conf: ConnectorsReadConf) extends SparkReader{

  override def init(spark: SparkSession) = {
    println("init jdbc reader...")
  }

  override def read(spark: SparkSession): DataFrame = {
    val url = conf.getReadConf("url")
    val table = conf.getReadConf("table")
    val username = conf.getReadConf("user")
    val password = conf.getReadConf("password")

    val properties = new Properties
    properties.put("user", username)
    properties.put("password", password)

    spark.read.jdbc(url, table, properties)
  }
}
```


## How to run

jdbc2jdbc, only need to add jdbc-assembly.jar to --jars

```
bin/spark-submit --driver-class-path /Users/zhengqh/.m2/repository/mysql/mysql-connector-java/5.1.40/mysql-connector-java-5.1.40.jar \
--jars /Users/zhengqh/spark-connectors-sbt/jdbc/target/scala-2.11/jdbc-assembly-0.0.1.jar \
--class com.zqh.spark.connectors.ConnectorClient \
--files /Users/zhengqh/spark-connectors-sbt/core/src/main/resources/application.conf \
--driver-java-options -Dconfig.file=/Users/zhengqh/spark-connectors-sbt/core/src/main/resources/application.conf \
/Users/zhengqh/spark-connectors-sbt/core/target/scala-2.11/core-assembly-0.0.1.jar
```

if execute in pipeline mode, you should add all related jar to --jars.
for example: reader is : [jdbc, cassandra], writer is [jdbc, mongodb].

configuration file:

```
connectors: [
  {
    "readers" :
      [
        {
          "type": "jdbc",
          "url": "jdbc:mysql://localhost/test",
          "table": "test",
          "user": "root",
          "password": "root"
        },
        {
          "type": "cassandra",
          "keyspace": "mykeyspace",
          "host": "192.168.6.70",
          "table": "user"
        }
      ]
  },
  {
    "writers" :
      [
        {
          "type": "jdbc",
          "url": "jdbc:mysql://localhost/test",
          "table": "test3",
          "user": "root",
          "password": "root",
          "mode": "overwrite"
        },
        {
          "type": "mongodb",
          "host": "localhost",
          "table": "test3",
          "db": "root",
          "mode": "overwrite"
        }
      ]
  }
]
```

run spark job:

```
bin/spark-submit --driver-class-path /Users/zhengqh/.m2/repository/mysql/mysql-connector-java/5.1.40/mysql-connector-java-5.1.40.jar \
--jars /Users/zhengqh/spark-connectors-sbt/jdbc/target/scala-2.11/jdbc-assembly-0.0.1.jar,/Users/zhengqh/spark-connectors-sbt/cassandra/target/scala-2.11/cassandra-assembly-0.0.1.jar,/Users/zhengqh/spark-connectors-sbt/mongodb/target/scala-2.11/mongodb-assembly-0.0.1.jar \
--class com.zqh.spark.connectors.ConnectorClient \
--files /Users/zhengqh/spark-connectors-sbt/core/src/main/resources/application.conf \
--driver-java-options -Dconfig.file=/Users/zhengqh/spark-connectors-sbt/core/src/main/resources/application.conf \
/Users/zhengqh/spark-connectors-sbt/core/target/scala-2.11/core-assembly-0.0.1.jar
```

## Ref

- <http://centerqi.github.io/java/2015/11/30/spark-scala-application-config>
