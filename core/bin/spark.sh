cd $SPARK_HOME
bin/spark-submit \
--jars /Users/zhengqh/spark-connectors-sbt/cassandra/target/scala-2.11/cassandra-assembly-0.0.1.jar \
--class com.zqh.spark.connectors.test.TestCassandraBySparkJars \
/Users/zhengqh/spark-connectors-sbt/core/target/scala-2.11/core-assembly-0.0.1.jar