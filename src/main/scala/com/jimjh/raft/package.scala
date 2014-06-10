package com.jimjh

/** Provides an implementation of the RAFT consensus algorithm.
  *
  * == Overview ==
  * TODO
  * @author Jim Lim - jim@quixey.com
  */
package object raft {

  // Configuration "module" for dependency injection. Not sure if this is a good place for it, but this sure is
  // convenient.
  class RaftServer(delegate: Application)
    extends ServerComponent
    with ConsensusServiceComponent
    with ClientServiceComponent
    with LogComponent {

    override val server = new Server
    override val consensusService = new ConsensusService
    override val clientService = new ClientService
    override val log = new Log(delegate)
  }

}