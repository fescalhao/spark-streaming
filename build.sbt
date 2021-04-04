name := "spark-streamming"
organization := "com.github.fescalhao"
version := "0.1"
scalaVersion := "2.12.10"
autoScalaLibrary := false

val sparkVersion = "3.1.1"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)

libraryDependencies ++= sparkDependencies