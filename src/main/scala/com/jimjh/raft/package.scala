package com.jimjh

import java.util.Properties

import com.jimjh.raft.log.LogComponent
import com.jimjh.raft.rpc.RaftConsensusService.FutureIface
import com.twitter.finagle.Thrift
import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory

/** Provides an implementation of the RAFT consensus algorithm.
  *
  * == Overview ==
  *
  * == Example Usage ==
  *
  * {{{
  *   val props = new Properties()
  *   props.put("node.id", s"localhost:$port")
  *   props.put("peers", "localhost:8080,localhost:8081,localhost:8082")
  *   val raft = new RaftServer(application, props)
  *   Await.ready(raft.start())
  * }}}
  *
  * @author Jim Lim - jim@jimjh.com
  */
package object raft {

  // TODO use more immutable objects, functional concurrency
  // TODO improve dependency injection, define public API, subclassing
  // TODO update scaladocs
  // TODO use autocloseable instead of start/stop

  type ReturnType = Option[Any]

  /** Configuration "module" for dependency injection.
    *
    * This is where we assemble the cake using dependency injection. Not sure if this is a good place for it, but
    * this sure is convenient.
    *
    * @param _delegate application _i.e._ the state machine RAFT controls
    * @param _props configuration properties
    */
  class RaftServer(_delegate: Application,
                   _props: Properties)
    extends ServerComponent
    with ConsensusServiceComponent
    with ClientServiceComponent
    with LogComponent
    with PersistenceComponent
    with ElectionTimerComponent {

    override val log = new Log(_delegate)

    override val persistence = new Persistence(_props)

    override val consensus: ConsensusService =
      new ConsensusService(
        _props,
        Thrift.newIface[FutureIface]
      )

    override protected val timer = new ElectionTimer(consensus)

    override protected[raft] val logger = Logger(LoggerFactory getLogger s"Raft:${consensus.id}")

    // XXX maybe pass a read-only interface of the Log?
    override val clientService = new ClientService
  }

  private[raft] def notNull(x: Any, n: String) = require(null != x, s"`$n` must not be null.")
}