package com.jimjh.raft

/** Wrapper for a RAFT server.
  *
  * <p>It looks odd because I am trying to use the Cake Pattern. It is meant to be used as follows:</p>
  *
  * {{{
  * object Server extends ServerComponent with ConsensusServiceComponent
  * }}}
  * @author Jim Lim - jim@quixey.com
  */
trait ServerComponent {
  this: ConsensusServiceComponent with ClientServiceComponent =>

  val server: Server

  /** RAFT Server
    *
    * <p>
    * Controls two Finagle Services - one for handling client requests, and one for handling requests from other
    * servers. Each server is implemented as a state machine that governs the transitions between {@code FOLLOWER},
    * {@code CANDIDATE}, and {@code LEADER}.
    * </p>
    */
  class Server {

    // TODO client service
    // TODO consensus service
    // TODO votedFor (persistent) - do we need one for each term?
    // TODO commitIndex
    // TODO nextIndex[]  (maybe group into some "leadership" object?)
    // TODO matchIndex[] (maybe group into some "leadership" object?)
    // TODO log

    /** Defines various possible states for the raft server. */
    object State extends Enumeration {
      type State = Value
      val Follower, Candidate, Leader = Value
    }

    import State._

    private[this] var _state: State = Follower

    def state: State = _state

  }

}
