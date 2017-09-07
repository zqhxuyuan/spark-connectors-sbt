
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