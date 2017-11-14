name := "database-benchmarks"
organization := "partners.campaign"
version := "1.0.0-SNAPSHOT"
scalaVersion := "2.12.4"

fork := true

libraryDependencies ++= Seq(
  "com.outr" %% "scarango-driver" % "0.8.4-SNAPSHOT",
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.1.0"
)