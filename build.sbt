name := "spark-connectors"

version := "1.0"

scalaVersion := "2.11.7"

import Dependencies._

lazy val api = (project in file("api")).settings(Common.settings: _*).settings(libraryDependencies ++= apiDependencies)

lazy val core = (project in file("core")).settings(Common.settings: _*).settings(libraryDependencies ++= coreDependencies).dependsOn(api)

lazy val jdbc = (project in file("jdbc")).settings(Common.settings: _*).settings(libraryDependencies ++= jdbcDependencies).dependsOn(api)

lazy val hdfs = (project in file("hdfs")).settings(Common.settings: _*).settings(libraryDependencies ++= hdfsDependencies).dependsOn(api)

lazy val hive = (project in file("hive")).settings(Common.settings: _*).settings(libraryDependencies ++= hiveDependencies).dependsOn(api)

lazy val cassandra = (project in file("cassandra")).settings(Common.settings: _*).settings(libraryDependencies ++= cassandraDependencies).dependsOn(api)

lazy val hbase = (project in file("hbase")).settings(Common.settings: _*).settings(libraryDependencies ++= hbaseDependencies).dependsOn(api)

lazy val mongodb = (project in file("mongodb")).settings(Common.settings: _*).settings(libraryDependencies ++= mongoDependencies).dependsOn(api)

lazy val codis = (project in file("codis")).settings(Common.settings: _*).settings(libraryDependencies ++= codisDependencies).dependsOn(api)

lazy val redis = (project in file("redis")).settings(Common.settings: _*).settings(libraryDependencies ++= redisDependencies).dependsOn(api)

lazy val elasticsearch = (project in file("elasticsearch")).settings(Common.settings: _*).settings(libraryDependencies ++= esDependencies).dependsOn(api)

lazy val jobserver = (project in file("jobserver")).settings(Common.settings: _*).settings(libraryDependencies ++= jobserverDependencies).dependsOn(api)

lazy val neo4j = (project in file("neo4j")).settings(Common.settings: _*).settings(libraryDependencies ++= neo4jDependencies).dependsOn(api)

lazy val orientdb = (project in file("orientdb")).settings(Common.settings: _*).settings(libraryDependencies ++= orientdbDependencies).dependsOn(api)

lazy val kafka = (project in file("kafka")).settings(Common.settings: _*).settings(libraryDependencies ++= kafkaDependencies).dependsOn(api)

lazy val root = (project in file(".")).settings(Common.settings: _*).
  aggregate(api, core, jdbc, hdfs, hive, cassandra, hbase, mongodb, codis, redis, elasticsearch)

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("io.netty.handler.**" -> "shadeio.io.netty.handler.@1").inAll,
  ShadeRule.rename("io.netty.channel.**" -> "shadeioi.io.netty.channel.@1").inAll,
  ShadeRule.rename("io.netty.util.**" -> "shadeio.io.netty.util.@1").inAll,
  ShadeRule.rename("io.netty.bootstrap.**" -> "shadeio.io.netty.bootstrap.@1").inAll,
  ShadeRule.rename("com.google.common.**" -> "shade.com.google.common.@1").inAll,
  ShadeRule.rename("com.google.protobuf.**" -> "shade.com.google.protobuf.@1").inAll
)

val meta = """META.INF(.)*""".r
assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", "unused", "UnusedStubClass.class") => MergeStrategy.discard
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".html" => MergeStrategy.first
  case n if n.startsWith("reference.conf") => MergeStrategy.concat
  case n if n.endsWith(".conf") => MergeStrategy.concat
  case meta(_) => MergeStrategy.discard
  case x => MergeStrategy.first
}

//mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
//  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//  case x => MergeStrategy.first
//}}
