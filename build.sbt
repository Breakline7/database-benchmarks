name := "database-benchmarks"
organization := "partners.campaign"
version := "1.0.0-SNAPSHOT"
scalaVersion := "2.12.4"
resolvers += Resolver.sonatypeRepo("releases")

fork := true

libraryDependencies ++= Seq(
  "com.outr" %% "scarango-driver" % "0.8.4",
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.1.0",
  "org.neo4j.driver" % "neo4j-java-driver" % "1.4.4"
)