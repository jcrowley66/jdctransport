name := "jdctransport"

version := "0.1"

val scalaVers = "2.12.8"
scalaVersion := scalaVers

val AkkaVersion     = "2.6.14"        // WARNING: Must match setting in resources/reference.conf
val AkkaHttpVersion = "10.2.4"
val sprayJsonVersion= "1.3.6"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % AkkaVersion,
  "com.typesafe.akka" %% "akka-http-spray-json" % AkkaHttpVersion

//  "io.spray" %% "spray-json" % sprayJsonVersion
)