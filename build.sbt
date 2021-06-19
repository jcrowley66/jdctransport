name := "jdctransport"

version := "0.1"

val scalaVers = "2.12.8"
scalaVersion := scalaVers

val AkkaVersion     = "2.6.14"        // WARNING: Must match setting in resources/reference.conf

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % AkkaVersion,
)