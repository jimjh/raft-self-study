package com.jimjh.raft

import com.jimjh.raft.log.LogComponent
import org.apache.commons.io.IOUtils

/** Wrapper for a RAFT server. Controls two Finagle Services - one for handling client requests, and one for handling
  * requests from other servers.
  *
  * It looks odd because I am trying to use the Cake Pattern. It is meant to be used as follows:
  *
  * {{{
  * object Server extends ServerComponent
  *   with ConsensusServiceComponent
  *   with ClientServiceComponent
  *   with LogComponent
  * }}}
  *
  * The resulting object will have a `consensusService`, a `clientService`, and a `log`.
  *
  * @author Jim Lim - jim@jimjh.com
  */
trait ServerComponent {
  this: ConsensusServiceComponent
    with ClientServiceComponent
    with LogComponent
    with PersistenceComponent
    with ElectionTimerComponent =>

  /** Starts various components.
    *
    * Including the log, the consensus service, and the client service.
    */
  def start() = {
    log.start()
    consensus.start()
  }

  def stop() = {
    IOUtils closeQuietly log
    IOUtils closeQuietly persistence
  }
}
