package com.jimjh.raft

import java.util.Properties

import com.twitter.finagle.Thrift
import com.twitter.util.Await
import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory

object TestMain {
  val logger = Logger(LoggerFactory getLogger "main")

  object DummyApplication extends Application {
    override def apply(cmd: String, args: Seq[String]) = {
      logger.info(s"received cmd: $cmd with args: ${args.mkString(",")}")
      None
    }
  }

  def main(args: Array[String]) {

    val servers = (8080 to 8082) map TestMain.newServer

    Thread.sleep(5000)
    logger.info("WAKE")

    servers.filter {
      case (raft, _) => raft.consensusService.isLeader
    }.map {
      case (raft, _) =>
        logger.info("Applying x on leader")
        raft.consensusService.apply("x", Nil)
        logger.info("Applying y on leader")
        raft.consensusService.apply("y", Nil)
        logger.info("Applying z on leader")
        raft.consensusService.apply("z", Nil)
    }

    servers.last match {
      case (_, server) => Await.ready(server) // block
    }
  }

  def newServer(port: Int) = {
    val props = new Properties()
    props.put("node.id", s"localhost:$port")
    props.put("peers", "localhost:8080,localhost:8081,localhost:8082")

    val raft = new RaftServer(DummyApplication, props)
    val server = Thrift.serveIface(s":$port", raft.consensusService)
    logger.info(s"launching consensus service @ $port...")
    raft.consensusService.start()
    (raft, server)
  }
}