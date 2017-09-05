import sbt._
import Keys._

object Common {
  val appVersion = "0.0.1"

  lazy val copyDependencies = TaskKey[Unit]("copy-dependencies")

  def copyDepTask = copyDependencies <<= (update, crossTarget, scalaVersion) map {
    (updateReport, out, scalaVer) =>
    updateReport.allFiles foreach { srcPath =>
      val destPath = out / "lib" / srcPath.getName
      IO.copyFile(srcPath, destPath, preserveLastModified=true)
    }
  }
  
  val settings: Seq[Def.Setting[_]] = Seq(
    version := appVersion,
    scalaVersion := "2.11.8",
    ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) },
    javacOptions ++= Seq("-source", "1.8", "-target", "1.8"), //, "-Xmx2G"),
    scalacOptions ++= Seq("-deprecation", "-unchecked"),
    copyDepTask,
    resolvers ++= Seq(
      // Resolver.mavenLocal has issues - hence the duplication
      "Local Maven Repository" at "file://"+Path.userHome.absolutePath+"/.m2/repository",
      //"Tongdun Maven" at "http://maven.fraudmetrix.cn/nexus/content/groups/public/",
      "Aliyun Maven" at "http://maven.aliyun.com/nexus/content/groups/public/"
      //"bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven/"

      //"Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",
      //"Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",
      //Classpaths.sbtPluginReleases,
      //DefaultMavenRepository
    )
  )
}
