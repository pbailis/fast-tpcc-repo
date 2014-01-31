import sbt._
import sbt.Classpaths.publishTask
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._
import scala.util.Properties

object VeloxBuild extends Build {
  val SCALAC_JVM_VERSION = "jvm-1.6"
  val JAVAC_JVM_VERSION = "1.6"

  lazy val root = Project(
    "root",
    file("."),
    settings = rootSettings
  ) aggregate (allProjects: _*) dependsOn(allDeps: _*)


  lazy val core = Project("core", file("core"), settings = coreSettings)

  lazy val client = Project("client", file("client"), settings = clientSettings) dependsOn(core)

  lazy val externalJassh = Project("external-jassh", file("external/jassh"), settings = externalJasshSettings)

  lazy val management = Project("management", file("management"), settings = managementSettings) dependsOn (externalJassh)

  lazy val assemblyProj = Project("assembly", file("assembly"), settings = assemblyProjSettings) dependsOn (packages: _*)

  lazy val packages = Seq[ClasspathDependency](core, client, management)
  lazy val packageProjects = Seq[ProjectReference](core, client, management)
  lazy val allExternal = Seq[ClasspathDependency](externalJassh)
  lazy val allExternalRefs = Seq[ProjectReference](externalJassh)
  lazy val allProjects = packageProjects ++ allExternalRefs ++ Seq[ProjectReference](assemblyProj)
  lazy val allDeps = packages ++ allExternal


  lazy val assembleDeps = TaskKey[Unit]("assemble-deps", "Build assembly of dependencies and packages Spark projects")

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
      "com.twitter" % "chill_2.10" % "0.3.5",
      "com.twitter" % "chill-bijection_2.10" % "0.3.5",
      "com.codahale.metrics" % "metrics-core" % "3.0.1",
      "org.scalatest" %% "scalatest" % "2.0" % "test"
    )
  )

  def externalJasshSettings = sharedSettings ++ Seq(
    name := "velox-external",

    resolvers ++= Seq(
      "JAnalyse Repository" at "http://www.janalyse.fr/repository/"
    ),

    libraryDependencies ++= Seq(
      "fr.janalyse" %% "janalyse-ssh" % "0.9.10" % "compile"
    )
  )

  def managementSettings = sharedSettings ++ Seq(
    name := "velox-management",

    libraryDependencies ++= Seq(
      "com.amazonaws" % "aws-java-sdk" % "1.2.1",
      "com.github.seratch" %% "awscala" % "0.1.3",
      "com.github.scopt" %% "scopt" % "3.2.0"
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