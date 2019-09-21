import sbtassembly.{Plugin},Plugin.AssemblyKeys._
import sbtassembly.Plugin._

name := "ruscello-spark"

organization := "org.ruscello"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++=Seq(
  "org.apache.spark" %% "spark-core" % "1.6.1" % "provided",
  "org.apache.spark" %% "spark-streaming" % "1.6.1",
  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.1",
  "org.apache.commons" % "commons-lang3" % "3.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.3.3",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.10" % "2.3.3",
  "junit" % "junit" % "4.7" % "test",
  "org.scalatest" % "scalatest_2.10" % "2.0" % "test",
  "mawazo" %% "hoidla" % "1.0",
  "org.chombo" %% "chombo-spark" % "1.0",
  "mawazo" %% "chombo" % "1.0",
  "gov.nist.math" % "jama" % "1.0.3"
)

net.virtualvoid.sbt.graph.Plugin.graphSettings

assemblySettings
