package com.jimjh.raft.spec

import java.util.Properties

import com.jimjh.raft._
import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.util.Random

/** Harness for integration tests.
  *
  * General Usage:
  *
  * {{{
  *   val servers = new Harness().withApp(app).build(10)
  * }}}
  *
  * @author Jim Lim - jim@jimjh.com
  */
class Harness {

  private[this] val _logger = Logger(LoggerFactory getLogger "Harness")

  private[this] var app: () => Application = null

  private[this] var first: Int = new Random().nextInt(500) + 9000

  def withApp(app: () => Application) = {
    this.app = app
    this
  }

  def withStartingPort(port: Int) = {
    this.first = port
    this
  }

  def build(num: Int) = {
    notNull(app, "app")
    val last = first + num - 1
    (first to last) map (node(_, peers(last)))
  }

  private[this] def node(port: Int, peers: String) = {
    val props = new Properties()
    props.put("node.id", s"localhost:$port")
    props.put("peers", peers)

    val raft = new RaftServer(app(), props)
    _logger.debug(s"launching consensus service @ $port...")
    raft.start()
    raft
  }

  private[this] def peers(last: Int) = (first to last).map(port => s"localhost:$port").mkString(",")
}
