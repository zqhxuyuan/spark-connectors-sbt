import sbt._
import Keys._

object Dependencies {
  val playVersion = "2.5.10"
  val sparkVersion = "2.2.0"
  val hadoopVersion = "2.8.1"

  val compileMode = System.getProperty("env", "compile")

  //Play自带的logback已经有日志实现
  excludeDependencies ++= Seq(
    SbtExclusionRule("org.slf4j", "slf4j-simple"),
    SbtExclusionRule("org.slf4j", "slf4j-api"),
    SbtExclusionRule("org.slf4j", "slf4j-jdk12"),
    SbtExclusionRule("commons-beanutils", "commons-beanutils-core"),
    SbtExclusionRule("commons-collections", "commons-collections"),
    SbtExclusionRule("commons-logging", "commons-logging"),
    SbtExclusionRule("org.slf4j", "slf4j-log4j12"),
    SbtExclusionRule("org.hamcrest", "hamcrest-core"),
    SbtExclusionRule("junit", "junit"),
    SbtExclusionRule("org.jboss.netty", "netty")
  )

  val commonDependencies: Seq[ModuleID] = Seq(
    "junit" % "junit" % "4.12" % "test",
    "org.scalatest" %% "scalatest" % "3.0.3" % "test",
    "com.google.guava" % "guava" % "19.0"
  )

  def sparkDependency(_sparkVersion: String = sparkVersion): Seq[ModuleID] = commonDependencies ++ Seq(
    "org.apache.spark" %% "spark-core" % _sparkVersion % compileMode,
    "org.apache.spark" %% "spark-sql" % _sparkVersion % compileMode
  )

  //=======================各个子模块的依赖========================
  val apiDependencies         : Seq[ModuleID] = sparkDependency()

  val coreDependencies       : Seq[ModuleID] = sparkDependency() ++ Seq(
    "com.typesafe" % "config" % "1.3.1",
    "com.alibaba" % "fastjson" % "1.2.31",
    "com.google.inject" % "guice" % "4.1.0"
  )

  val jdbcDependencies        : Seq[ModuleID] = sparkDependency() ++ Seq(
    "mysql" % "mysql-connector-java" % "5.1.38" % "compile"
  )

  val hdfsDependencies        : Seq[ModuleID] = sparkDependency() ++ Seq(
    "org.apache.hadoop" % "hadoop-client" % hadoopVersion,
    "org.apache.hadoop" % "hadoop-hdfs" % hadoopVersion,
    "org.apache.hadoop" % "hadoop-common" % hadoopVersion
  )

  val hiveDependencies        : Seq[ModuleID] = sparkDependency() ++ Seq(
    "org.apache.spark" %% "spark-hive" % sparkVersion % "compile"
  )

  val cassandraDependencies   : Seq[ModuleID] = sparkDependency() ++ Seq(
    "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.5"
  )

  val hbaseDependencies       : Seq[ModuleID] = sparkDependency() ++ Seq(

  )

  val mongoDependencies       : Seq[ModuleID] = sparkDependency() ++ Seq(
    "org.mongodb.spark" %% "mongo-spark-connector" % sparkVersion
  )

  val codisDependencies       : Seq[ModuleID] = sparkDependency() ++ Seq(
    "io.codis.jodis" % "jodis" % "0.4.1",
    "redis.clients" % "jedis" % "2.9.0"
  )

  val redisDependencies       : Seq[ModuleID] = sparkDependency() ++ Seq(
    "redis.clients" % "jedis" % "2.9.0"
  )

  val esDependencies          : Seq[ModuleID] = sparkDependency() ++ Seq(
    //"org.elasticsearch" % "elasticsearch-hadoop" % "5.5.2"
  )

  val jobserverDependencies   : Seq[ModuleID] = sparkDependency() ++ Seq(

  )

  val neo4jDependencies       : Seq[ModuleID] = sparkDependency() ++ Seq(
    "neo4j-contrib" % "neo4j-spark-connector" % "2.1.0-M4"
  )

  val orientdbDependencies    : Seq[ModuleID] = sparkDependency() ++ Seq(

  )

  val kafkaDependencies    : Seq[ModuleID] = sparkDependency() ++ Seq(

  )

  val webDependencies         : Seq[ModuleID] = sparkDependency() ++ Seq(
    "com.typesafe.play" %% "play" % playVersion,
    "com.typesafe.play" %% "play-jdbc" % playVersion,
    "com.typesafe.play" %% "play-ws" % playVersion,
    "com.typesafe.play" %% "play-cache" % playVersion,
    "com.typesafe.play" %% "play-jdbc-evolutions" % playVersion,
    "com.typesafe.play" %% "anorm" % "2.4.0",
    "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.7.6",
    "org.webjars" % "bootstrap" % "3.3.4",
    "org.webjars" % "metisMenu" % "1.1.3",
    "org.webjars" % "morrisjs" % "0.5.1",
    "org.webjars" % "font-awesome" % "4.3.0",
    "org.webjars" % "jquery" % "2.1.3",
    "org.webjars" % "flot" % "0.8.3",
    "org.webjars" % "datatables" % "1.10.5",
    "org.webjars" % "datatables-plugins" % "1.10.5",
    "com.adrianhurt" % "play-bootstrap_2.11" % "1.1-P25-B3"
  )
}
