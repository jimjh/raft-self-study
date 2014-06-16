package com.jimjh.raft

import com.twitter.util.Await
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.slf4j.Logger
import com.twitter.finagle.Thrift
import java.util.Properties

object Main {
  val logger = Logger(LoggerFactory getLogger "main")

  object DummyApplication extends Application {
    override def apply(cmd: String, args: Array[String]) {
      logger.info(s"received cmd: $cmd with args: ${args.mkString(",")}")
    }
  }

  def main(args: Array[String]) {
    logger.trace("#main started")

    val servers = (8080 to 8082).map {
      case port =>
        val props = new Properties()
        props.put("node.id", s"localhost:$port")
        props.put("peers", "localhost:8080,localhost:8081,localhost:8082")
        val raft = new RaftServer(DummyApplication, props)
        val server = Thrift.serveIface(s":$port", raft.consensusService)
        logger.trace(s"launching consensus service @ $port...")
        raft.consensusService.start()
        server
    }

    Await.ready(servers.last)
  }
}
