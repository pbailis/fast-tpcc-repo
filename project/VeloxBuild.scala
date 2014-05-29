import sbt._
import sbt.Classpaths.publishTask
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._
import scala.util.Properties

object VeloxBuild extends Build {
  val SCALAC_JVM_VERSION = "jvm-1.7"
  val JAVAC_JVM_VERSION = "1.7"

  lazy val root = Project(
    "root",
    file("."),
    settings = rootSettings
  ) aggregate (allProjects: _*) dependsOn(allDeps: _*)


  lazy val core = Project("core", file("core"), settings = coreSettings)

  lazy val client = Project("client", file("client"), settings = clientSettings) dependsOn(core)

  lazy val assemblyProj = Project("assembly", file("assembly"), settings = assemblyProjSettings) dependsOn (packages: _*)

  lazy val packages = Seq[ClasspathDependency](core, client)
  lazy val packageProjects = Seq[ProjectReference](core, client)
  lazy val allProjects = packageProjects ++ Seq[ProjectReference](assemblyProj)
  lazy val allDeps = packages


  lazy val assembleDeps = TaskKey[Unit]("assemble-deps", "Build assembly of dependencies and packages Velox projects")

  def sharedSettings = Defaults.defaultSettings ++ Seq(
    organization := "edu.berkeley.velox",
    version := "0.1",
    scalaVersion := "2.10.3",
    scalacOptions := Seq("-feature", "-deprecation", "-target:" + SCALAC_JVM_VERSION),
    javacOptions := Seq("-target", JAVAC_JVM_VERSION, "-source", JAVAC_JVM_VERSION),
    unmanagedJars in Compile <<= baseDirectory map {
      base => (base / "lib" ** "*.jar").classpath
    },

    libraryDependencies ++= Seq(
      "org.slf4j" % "slf4j-api" % "1.7.2",
      "org.slf4j" % "slf4j-log4j12" % "1.7.2",
      "org.scalatest" %% "scalatest" % "2.0"  % "test",
      "com.typesafe" %% "scalalogging-slf4j" % "1.0.1"
    )
  )

  def rootSettings = sharedSettings ++ Seq(
    publish := {}
  )

  def coreSettings = sharedSettings ++ Seq(
    name := "velox-core",

    libraryDependencies ++= Seq(
      "com.github.scopt" %% "scopt" % "3.2.0",
      "com.esotericsoftware.kryo" % "kryo" % "2.23.0",
      "com.twitter" % "chill_2.10" % "0.3.5",
      "com.twitter" % "chill-bijection_2.10" % "0.3.5",
      "com.codahale.metrics" % "metrics-core" % "3.0.1",
      "org.scalatest" %% "scalatest" % "2.0" % "test",
      "org.apache.curator" % "curator-framework" % "2.3.0",
      "org.apache.curator" % "curator-recipes" % "2.3.0",
      "org.apache.curator" % "curator-test" % "2.3.0",
      "org.scalanlp" % "breeze-math_2.10" % "0.4"
    )
  )

  def clientSettings = sharedSettings ++ Seq(
    name := "velox-client"
  )

  def assemblyProjSettings = sharedSettings ++ Seq(
    name := "velox-assembly",
    assembleDeps in Compile <<= (packageProjects.map(packageBin in Compile in _) ++ Seq(packageDependency in Compile)).dependOn,
    jarName in assembly <<= version map { v => "velox-assembly-" + v + ".jar" }
  ) ++ assemblySettings ++ extraAssemblySettings

  def extraAssemblySettings() = Seq(
    test in assembly := {},
    mergeStrategy in assembly := {
      case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
      case m if m.toLowerCase.matches("meta-inf.*\\.sf$") => MergeStrategy.discard
      case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
      case "reference.conf" => MergeStrategy.concat
      case _ => MergeStrategy.first
    }
  )

}
