package com.jimjh.raft

import com.twitter.finagle.Http
import com.twitter.util.Await
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.slf4j.Logger

object Main {
  val logger = Logger(LoggerFactory getLogger "main")

  object DummyApplication extends Application {
    override def apply(cmd: String, args: Array[String]) {
      logger.info(s"received cmd: $cmd with args: ${args.mkString(",")}")
    }
  }

  def main(args: Array[String]) {
    logger.trace("#main started")
    val raftServer = new RaftServer(DummyApplication)
    val server = Http.serve(":8080", raftServer.consensusService)

    logger.trace("launching consensus service ...")
    Await.ready(server)
  }
}
