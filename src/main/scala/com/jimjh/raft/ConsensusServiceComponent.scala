package com.jimjh.raft

import com.twitter.util.Future
import org.jboss.netty.handler.codec.http.{HttpResponseStatus, DefaultHttpResponse}
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.slf4j.Logger
import com.jimjh.raft.rpc.{Vote, RaftConsensusService}
import java.util.Properties

/** Wrapper for a Consensus Service.
  *
  * It looks odd because I am trying to use the Cake Pattern.
  *
  * @author Jim Lim - jim@jimjh.com
  */
trait ConsensusServiceComponent {

  val consensusService: ConsensusService

  /** Responds to RPCs from other servers.
    *
    * Each server is implemented as a state machine that governs the transitions between
    * `FOLLOWER`, `CANDIDATE`, and `LEADER`.
    *
    * @param props configuration options for the consensus service
    */
  class ConsensusService(private[this] val props: Properties)
    extends RaftConsensusService[Future]
    with ElectionTimerDelegate {

    /** Defines various possible states for the raft server. */
    object State extends Enumeration {
      type State = Value
      val Follower, Candidate, Leader = Value
    }

    import State._

    private[this] val _logger = Logger(LoggerFactory getLogger "ConsensusService")
    private[this] var _state: State = Follower
    private[this] val _timer: ElectionTimer = new ElectionTimer(this)
    private[this] val _id: Long = (props getProperty "node.id").toLong

    // TODO persistent state
    private[this] var _term: Long = 0
    private[this] var _votedFor: Long = 0
    private[this] var _votesReceived: List[Long] = List[Long]()

    def state: State = _state

    /** BEGIN RPC API **/

    override def requestVote(term: Long,
                             candidateId: Long,
                             lastLogIndex: Long,
                             lastLogTerm: Long): Future[Vote] =
    // TODO wait for RequestVoteRPCs, AppendEntriesRPC
      Future.value(Vote.apply(0, false))

    /** END RPC API */

    override def timeout() {
      this.synchronized {
        _logger.debug(s"Timeout triggered. Current state is $state.")
        state match {
          case Follower => startElection
          case Candidate => // TODO start new election
        }
      }
    }

    def start() = {
      _logger.info(s"Launching consensus service - $props")
      _timer.start()
    }

    /**
     * Starts an election, assuming that it has a lock on the state of the server.
     */
    private[this] def startElection = {
      _logger.debug("Starting an election.")
      _state = Candidate
      _term += 1
      _votedFor = _id
      _votesReceived = List[Long](_id)
      // TODO send out RequestVoteRPCs to members of the cluster
      start()
      this
    }
  }

}
