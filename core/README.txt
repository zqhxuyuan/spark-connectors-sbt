
jdbc to jdbc:

```
projectRoot="/Users/zhengqh/spark-connectors-sbt"
configFile="$projectRoot/core/src/main/resources/jdbc.conf"
depJars="/Users/zhengqh/spark-connectors-sbt/jdbc/target/scala-2.11/jdbc-assembly-0.0.1.jar"
mysql="/Users/zhengqh/.m2/repository/mysql/mysql-connector-java/5.1.40/mysql-connector-java-5.1.40.jar"
```

jdbc to codis:

```
projectRoot="/Users/zhengqh/spark-connectors-sbt"
configFile="$projectRoot/core/src/main/resources/codis.conf"
depJars="$projectRoot/codis/target/scala-2.11/codis-assembly-0.0.1.jar,/Users/zhengqh/spark-connectors-sbt/jdbc/target/scala-2.11/jdbc-assembly-0.0.1.jar"
```

```
bin/spark-submit --master local \
--class com.zqh.spark.connectors.ConnectorClient \
--jars $depJars --files $configFile \
--driver-class-path $mysql --driver-java-options -Dconfig.file=$configFile \
$projectRoot/core/target/scala-2.11/core-assembly-0.0.1.jar
```

  * 1. sbt jdbc/assembly -Denv=provided
  * 2. sbt core/assembly -Denv=provided
  * 3. run spark-submit
  *
  * Notice:
  *
  * 1. mysql driver jar add to driver class path, not to --jars
  * 2. dependencies jar which is jdbc-assembly.jar add to --jars
  * 3. running jar which is core-assembly is the resources
  *
  *bin/spark-submit --driver-class-path /Users/zhengqh/.m2/repository/mysql/mysql-connector-java/5.1.40/mysql-connector-java-5.1.40.jar \
  *--jars /Users/zhengqh/spark-connectors-sbt/jdbc/target/scala-2.11/jdbc-assembly-0.0.1.jar \
  *--class com.zqh.spark.connectors.test.TestJdbcBySparkJars \
  */Users/zhengqh/spark-connectors-sbt/core/target/scala-2.11/core-assembly-0.0.1.jar

  * Question:
  * 1. mysql driver already exist in jdbc-assembly.jar, and we add jdbc-assembly to --jars,
  * If without adding mysql driver to driver class path, still can't find suitable driver..


bin/spark-submit \
  *--jars /Users/zhengqh/spark-connectors-sbt/cassandra/target/scala-2.11/cassandra-assembly-0.0.1.jar \
  *--class com.zqh.spark.connectors.test.TestCassandraBySparkJars \
  */Users/zhengqh/spark-connectors-sbt/core/target/scala-2.11/core-assembly-0.0.1.jar


projectRoot="/Users/zhengqh/spark-connectors-sbt"
configFile="$projectRoot/core/src/main/resources/jdbc.conf"
mysql="/Users/zhengqh/.m2/repository/mysql/mysql-connector-java/5.1.40/mysql-connector-java-5.1.40.jar"

bin/spark-submit --master local \
--class com.zqh.spark.connectors.ConnectorSimpleClient \
--files $configFile \
--driver-class-path $mysql --driver-java-options -Dconfig.file=$configFile \
$projectRoot/core/target/scala-2.11/core-assembly-0.0.1.jar
