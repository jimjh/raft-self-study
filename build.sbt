name := "RAFT"

version := "1.0"

scalaVersion := "2.10.3"

libraryDependencies += "com.twitter" %% "finagle-http" % "6.2.0"

libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging-slf4j" % "2.1.2"

libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.0-rc1" % "runtime"

libraryDependencies += "org.apache.logging.log4j" % "log4j-slf4j-impl" % "2.0-rc1" % "runtime"