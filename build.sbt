name := "noworlater"

version := "1.0"

scalaVersion := "2.12.4"

// https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-kinesis
libraryDependencies ++= Seq(

  "ch.qos.logback" % "logback-classic" % "1.0.13",

  "com.amazonaws" % "aws-java-sdk-kinesis" % "1.11.218",
  "net.debasishg" %% "redisclient" % "3.4",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.6.6",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.6",
  "com.google.guava" % "guava" % "21.0",
  "com.typesafe.scala-logging" % "scala-logging_2.12" % "3.5.0" exclude("org.scala-lang", "scala-reflect")
)


    