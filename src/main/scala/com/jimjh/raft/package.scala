package com.jimjh

import java.util.Properties

import com.jimjh.raft.log.LogComponent
import com.jimjh.raft.rpc.RaftConsensusService.FutureIface
import com.twitter.finagle.Thrift

/** Provides an implementation of the RAFT consensus algorithm.
  *
  * == Overview ==
  *
  * @author Jim Lim - jim@jimjh.com
  */
package object raft {

  // TODO use more immutable objects, functional concurrency
  // TODO maybe I need monads?
  // TODO improve dependency injection, define public API, subclassing
  // TODO annotate methods that are open for tests only
  // TODO update scaladocs

  type ReturnType = Option[Any]

  /** Configuration "module" for dependency injection. Not sure if this is a good place for it, but
    * this sure is convenient.
    *
    * @param _delegate application _i.e._ the state machine RAFT controls
    * @param _props configuration properties
    */
  class RaftServer(private[this] val _delegate: Application,
                   private[this] val _props: Properties)
    extends ServerComponent
    with ConsensusServiceComponent
    with ClientServiceComponent
    with LogComponent
    with ElectionTimerComponent {

    override val log = new Log(_delegate)

    override val consensusService: ConsensusService =
      new ConsensusService(
        _props,
        log,
        new ElectionTimer(consensusService),
        Thrift.newIface[FutureIface]
      )

    // XXX maybe pass a read-only interface of the Log?
    override val clientService = new ClientService

    def start() = {
      consensusService.start()
      this
    }
  }

  private[raft] def notNull(x: Any, n: String) = require(null != x, s"$n must not be null.")
}