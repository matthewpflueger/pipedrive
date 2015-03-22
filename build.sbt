name := "pipedrive"

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
    "ch.qos.logback" % "logback-classic" % "1.0.13",

    "com.lihaoyi" %% "upickle" % "0.2.6",

    "com.typesafe.akka" %% "akka-actor" % "2.3.9",
    "com.typesafe.akka" %% "akka-slf4j" % "2.3.9",

    "com.typesafe.akka" % "akka-stream-experimental_2.11" % "1.0-M4",
    "com.typesafe.akka" % "akka-http-experimental_2.11" % "1.0-M4",
    "com.typesafe.akka" % "akka-http-core-experimental_2.11" % "1.0-M4"
)
