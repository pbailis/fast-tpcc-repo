name := "velox"

version := "0.1"

scalaVersion := "2.10.3"

scalacOptions += "-deprecation"

scalacOptions += "-feature"

unmanagedSourceDirectories in Compile <+= baseDirectory(_ / "contrib/jassh/src")

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.2",
  "org.slf4j" % "slf4j-log4j12" % "1.7.2",
  "com.github.scopt" %% "scopt" % "3.2.0",
  "com.twitter" % "chill_2.10" % "0.3.5",
  "com.twitter" % "chill-bijection_2.10" % "0.3.5",
  "com.typesafe" %% "scalalogging-slf4j" % "1.0.1",
  "com.codahale.metrics" % "metrics-core" % "3.0.1",
  "com.amazonaws" % "aws-java-sdk" % "1.2.1",
  "com.github.seratch" %% "awscala" % "0.1.3",
  "fr.janalyse" %% "janalyse-ssh" % "0.9.10" % "compile",
  "org.scalatest" %% "scalatest" % "2.0" % "test"
)

resolvers += "JAnalyse Repository" at "http://www.janalyse.fr/repository/"
