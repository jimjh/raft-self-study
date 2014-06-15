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

    val props1 = new Properties()
    props1.put("node.id", "localhost:8080")
    props1.put("peers", "localhost:8081")

    val raft1 = new RaftServer(DummyApplication, props1)
    val server1 = Thrift.serveIface(":8080", raft1.consensusService)
    raft1.consensusService.start()

    val props2 = new Properties()
    props2.put("node.id", "localhost:8081")
    props2.put("peers", "localhost:8080")

    val raft2 = new RaftServer(DummyApplication, props2)
    val server2 = Thrift.serveIface(":8081", raft2.consensusService)
    raft2.consensusService.start()

    logger.trace("launching consensus service ...")
    Await.ready(server1)
  }
}
