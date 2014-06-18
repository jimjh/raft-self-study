package com.jimjh

import java.util.Properties
import com.twitter.finagle.Thrift
import com.jimjh.raft.rpc.RaftConsensusService.FutureIface

/** Provides an implementation of the RAFT consensus algorithm.
  *
  * == Overview ==
  * TODO
  * @author Jim Lim - jim@jimjh.com
  */
package object raft {

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

    /* This all looks really odd to me; is it a good idea? */

    override val log = new Log(_delegate)

    override val consensusService: ConsensusService =
      new ConsensusService(
        _props,
        log,
        new ElectionTimer(consensusService),
        Thrift.newIface[FutureIface]
      )

    // XXX what does this do?
    override val server = new Server

    // XXX maybe pass a read-only interface of the Log?
    override val clientService = new ClientService
  }

}