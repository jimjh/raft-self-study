package com.jimjh.raft

import com.twitter.util.{Try, Promise, Future}
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.slf4j.Logger
import com.jimjh.raft.rpc.{Vote, RaftConsensusService}
import java.util.Properties
import com.twitter.finagle.Thrift
import com.jimjh.raft.rpc.RaftConsensusService.FutureIface

/** Wrapper for a Consensus Service.
  *
  * It looks odd because I am trying to use the Cake Pattern.
  *
  * @author Jim Lim - jim@jimjh.com
  */
trait ConsensusServiceComponent {

  val consensusService: ConsensusService

  /** Responds to RPCs from other servers using the RAFT algorithm.
    *
    * Each server is implemented as a state machine that governs the transitions between
    * `FOLLOWER`, `CANDIDATE`, and `LEADER`. The `host:port` of each node will be treated as its
    * node ID _i.e._ each node is expected be restarted with the same `host:port`.
    *
    * TODO review code for error handling, logging, scala idioms etc.
    *
    * @define assumeLock Assumes that it has a lock on the state of the consensus service.
    * @define takeLock Takes a lock on the state of the consensus service.
    * @param _props configuration options for the consensus service
    */
  class ConsensusService(private[this] val _props: Properties,
                         private[this] val _log: LogComponent#Log) // XXX this seems like a bad idea
    extends RaftConsensusService[Future]
    with ElectionTimerDelegate {

    /** Defines various possible states for the raft server. */
    object State extends Enumeration {
      type State = Value
      val Follower, Candidate, Leader = Value
    }

    import State._

    /** Node ID */
    private[this] val _id: String = _props getProperty "node.id"

    private[this] var _state: State = Follower
    private[this] val _logger = Logger(LoggerFactory getLogger s"ConsensusService:${_id}")
    private[this] val _timer: ElectionTimer = new ElectionTimer(this)

    /** Map of node IDs to thrift clients */
    private[this] val _peers = extractPeers(_props getProperty "peers")

    // TODO persistent state
    private[this] var _term: Long = 0
    private[this] var _votedFor: Option[String] = None
    private[this] var _votesReceived: Set[String] = Set.empty[String]

    def state: State = _state

    /** BEGIN RPC API **/

    /** $takeLock */
    override def requestVote(term: Long,
                             candidateId: String,
                             lastLogIndex: Long,
                             lastLogTerm: Long): Future[Vote] = {
      // TODO wait for RequestVoteRPCs, AppendEntriesRPC
      _logger.debug(s"Received RequestVote($term, $candidateId, $lastLogIndex, $lastLogTerm)")
      _timer.start()
      new Promise(Try {
        ConsensusService.this.synchronized {
          val granted = shouldVote(term, candidateId, lastLogIndex, lastLogTerm)
          Vote(term, granted)
        }
      })
    }

    /** END RPC API */

    /** Triggered by [[ElectionTimer]]
      *
      * $takeLock
      */
    override def timeout() {
      this.synchronized {
        _logger.debug(s"Election timeout triggered. Current state is $state.")
        state match {
          case Follower | Candidate => startElection
        }
      }
    }

    def start() = {
      _logger.info(s"Starting consensus service - ${_props}")
      _timer.start()
    }

    /** Starts an election and restarts the [[ElectionTimer]].
      *
      * $assumeLock
      */
    private[this] def startElection = {
      _logger.debug("Starting an election.")
      _state = Candidate
      _term += 1
      _votedFor = Some(_id)
      _votesReceived = Set(_id)
      requestVotes()
      _timer.start()
      this
    }

    /** Sends a RequestVote RPC to each node.
      *
      * $assumeLock
      */
    private[this] def requestVotes() = {
      val last = _log.last
      _peers.foreach {
        // XXX how to handle errors? retries?
        case (id, client) =>
          _logger.debug(s"Sending RequestVote RPC to $id")
          client.requestVote(_term, _id, last.term, last.index)
            .onSuccess(tallyVotes(id, _))
            .onFailure {
            exc => _logger.error(s"RequestVote failure: $exc")
          }
      }
    }

    /** Adds the given vote to the current tally.
      *
      * $takeLock
      * @param id   voter ID
      * @param vote (`term`, `granted`)
      */
    private[this] def tallyVotes(id: String, vote: Vote) {
      _logger.debug(s"RequestVote success: $vote")
      this.synchronized {
        if (vote == Vote(_term, granted = true)) {
          _votesReceived += id
          if (_votesReceived.size >= requiredMajority) becomeLeader()
        }
      }
    }

    /** Sets state to [[Leader]].
      *
      * $assumeLock
      */
    private[this] def becomeLeader() = _state = Leader

    /** Determines if given request should receive vote.
      *
      * $assumeLock
      */
    private[this] def shouldVote(term: Long,
                                 candidateId: String,
                                 lastLogIndex: Long,
                                 lastLogTerm: Long) = {
      val last = _log.last
      (term >= _term) &&
        (_votedFor.isEmpty || _votedFor == Some(candidateId)) &&
        lastLogTerm >= last.term &&
        lastLogIndex >= last.index
    }

    /** $assumeLock
      * @return minimum number of nodes required for majority
      */
    private[this] def requiredMajority = _peers.size / 2.0

    /** Converts comma-separated host ports into a map. */
    private[this] def extractPeers(prop: String): Map[String, RaftConsensusService[Future]] = {
      (prop split ",").map {
        case id =>
          // XXX how to handle errors? mismatches? missing colons?
          val client = Thrift.newIface[FutureIface](id)
          (id, client)
      }.toMap
    }
  }

}
