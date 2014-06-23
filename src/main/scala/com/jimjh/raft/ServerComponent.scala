package com.jimjh.raft

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
    with ElectionTimerComponent =>
}
