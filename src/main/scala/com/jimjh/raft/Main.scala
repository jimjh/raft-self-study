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

    val props = new Properties()
    props.put("node.id", "0");

    val raftServer = new RaftServer(DummyApplication, props)
    val server = Thrift.serveIface(":8080", raftServer.consensusService)

    logger.trace("launching consensus service ...")
    raftServer.consensusService.start()
    Await.ready(server)
  }
}
