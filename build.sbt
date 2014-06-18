name := "RAFT"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "com.twitter" %% "finagle-thrift" % "6.17.0"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"

libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.0-rc1" % "test"

libraryDependencies += "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.0-rc1" % "test"

libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.2.0" % "test"

com.twitter.scrooge.ScroogeSBT.newSettings

libraryDependencies ++= Seq(
  "org.apache.thrift" % "libthrift" % "0.9.1",
  "com.twitter" %% "scrooge-core" % "3.16.0"
)

scalacOptions ++= Seq("-feature")