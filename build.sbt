name := "spark-streamming"
organization := "com.github.fescalhao"
version := "0.1"
scalaVersion := "2.12.10"
autoScalaLibrary := false

val sparkVersion = "3.1.1"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-avro" % sparkVersion,
  "com.datastax.spark" %% "spark-cassandra-connector" % "3.0.1",
  "joda-time" % "joda-time" % "2.10.10"
)

libraryDependencies ++= sparkDependencies