package com.jimjh.raft

import com.twitter.util.{Try, Promise, Future}
import org.slf4j.LoggerFactory
import com.typesafe.scalalogging.slf4j.Logger
import com.jimjh.raft.rpc.{Entry, Vote, RaftConsensusService}
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
    with ElectionTimerDelegate
    with HeartBeatDelegate {

    /** Defines various possible states for the raft server. */
    object State extends Enumeration {
      type State = Value
      val Follower, Candidate, Leader = Value
    }

    import State._

    class Term(val term: Long, var votedFor: Option[String]) {
      def <=(other: Long) = term <= other

      def <(other: Long) = term < other

      def compare(other: Long) = term.compare(other)

      override def toString = s"$term"
    }

    /** Node ID */
    private[this] val _id: String = _props getProperty "node.id"

    private[this] var _state: State = Follower
    private[this] val _logger = Logger(LoggerFactory getLogger s"ConsensusService:${_id}")
    private[this] val _timer: ElectionTimer = new ElectionTimer(this)
    private[this] var _heartBeat: Option[HeartBeat] = None

    /** Map of node IDs to thrift clients */
    private[this] val _peers = extractPeers(_props getProperty "peers")

    // TODO persistent state
    private[this] var _term: Term = new Term(0, None)
    private[this] var _votesReceived: Set[String] = Set.empty[String]

    def state: State = _state

    /* BEGIN RPC API **/

    /** Invoked by candidates to gather votes.
      *
      * $takeLock
      * @return vote
      */
    override def requestVote(term: Long,
                             candidateId: String,
                             lastLogIndex: Long,
                             lastLogTerm: Long): Future[Vote] = {
      _logger.debug(s"Received RequestVote($term, $candidateId, $lastLogIndex, $lastLogTerm)")
      new Promise(Try {
        ConsensusService.this.synchronized {
          if (_term < term) becomeFollower(term)
          val granted = shouldVote(term, candidateId, lastLogIndex, lastLogTerm)
          if (granted) {
            _term.votedFor = Some(candidateId)
            _timer.restart() // avoid starting an election if one is in progress (optional)
          }
          Vote(term, granted)
        }
      })
    }

    /** Invoked by leader to replicate log entries; also acts as heartbeat.
      *
      * $takeLock
      * @return true iff accepted
      */
    override def appendEntries(term: Long,
                               leaderId: String,
                               prevLogIndex: Long,
                               prevLogTerm: Long,
                               entries: Seq[Entry],
                               leaderCommit: Long): Future[Boolean] = {
      _logger.trace(s"Received AppendEntries($term, $leaderId, $prevLogIndex, $prevLogTerm)")
      new Promise(Try {
        ConsensusService.this.synchronized {
          state match {
            case Follower => updateFollower(term)
            case Candidate => updateCandidate(term)
            case Leader => updateLeader(term)
          }
        }
      })
    }

    /* END RPC API */

    /** Triggered by [[ElectionTimer]].
      *
      * $takeLock
      */
    override def timeout() {
      this.synchronized {
        _logger.info(s"Election timeout triggered. Current state is $state for term ${_term}.")
        state match {
          case Follower | Candidate => becomeCandidate()
        }
      }
    }

    /** Triggered by [[HeartBeat]]. Sends an empty AppendEntries RPC to each node. */
    override def pulse(term: Long) {
      _peers.values.foreach(_.appendEntries(term, _id, -1, -1, Nil, -1))
    }

    def start() = {
      _logger.info(s"Starting consensus service - ${_props}")
      _timer.restart()
    }

    /** Sends a RequestVote RPC to each node. */
    private[this] def requestVotes(term: Long, logTerm: Long, logIndex: Long) {
      _peers.foreach {
        case (id, client) =>
          _logger.debug(s"Sending RequestVote RPC to $id")
          client.requestVote(term, _id, logTerm, logIndex)
            .onSuccess(tallyVotes(id, _))
            .onFailure(_logger.error(s"RequestVote failure.", _))
      }
    }

    /** Adds the given vote to the current tally.
      *
      * $takeLock
      * @param id   voter ID
      * @param vote (`term`, `granted`)
      */
    private[this] def tallyVotes(id: String, vote: Vote) {
      _logger.debug(s"RequestVote success from $id: $vote")
      this.synchronized {
        if (_state == Candidate && vote == Vote(_term.term, granted = true)) {
          _votesReceived += id
          if (_votesReceived.size >= requiredMajority) becomeLeader()
        }
      }
    }

    /** Starts [[_heartBeat]].
      * $assumeLock
      *
      * @param term term for each this node holds leadership
      */
    private[this] def startHeartBeat(term: Long) {
      _heartBeat = Some(new HeartBeat(this, term).start())
    }

    /** $assumeLock */
    private[this] def stopHeartBeat() {
      _heartBeat map (_.cancel())
      _heartBeat = None
    }

    /** Sets state to [[Leader]].
      *
      * $assumeLock
      */
    private[this] def becomeLeader() {
      _logger.info(s"(state: ${_state}, term: ${_term}) ~> (state: Leader, term: ${_term})")

      _state = Leader
      startHeartBeat(_term.term)
      _timer.cancel()
    }

    /** Starts an election and restarts the [[ElectionTimer]].
      *
      * $assumeLock
      */
    private[this] def becomeCandidate() {
      _state = Candidate
      _term = new Term(_term.term + 1, Some(_id))
      _logger.info(s"Starting an election for term ${_term}.")

      _votesReceived = Set(_id)

      val last = _log.last
      requestVotes(_term.term, last.term, last.index)

      _timer.restart()
    }

    /** Sets state to [[Follower]], resetting the term if necessary.
      *
      * $assumeLock
      */
    private[this] def becomeFollower(term: Long) {
      _logger.info(s"(state: ${_state}, term: ${_term}) ~> (state: Follower, term: $term)")

      _term compare term match {
        case -1 => _term = new Term(term, None)
        case 0 => // do nothing
        case 1 => throw new IllegalStateException(s"#becomeFollower($term}) was invoked in term ${_term}.")
      }

      _state = Follower
      stopHeartBeat() // no-op if not leader
      _timer.restart()
    }

    /** $assumeLock */
    private[this] def updateLeader(term: Long): Boolean = {
      _term < term match {
        case true =>
          becomeFollower(term)
          true
        case false => false
      }
    }

    /** $assumeLock */
    private[this] def updateCandidate(term: Long): Boolean = {
      _term <= term match {
        case true =>
          becomeFollower(term)
          true
        case false => false
      }
    }

    /** $assumeLock */
    private[this] def updateFollower(term: Long): Boolean = {
      _term <= term match {
        case true =>
          _timer.restart()
          true
        case false => false
      }
    }

    /** $assumeLock
      * @return true iff the given request deserves a vote
      */
    private[this] def shouldVote(term: Long,
                                 candidateId: String,
                                 lastLogIndex: Long,
                                 lastLogTerm: Long): Boolean = {
      _term compare term match {
        case -1 => false // shouldn't happen, since term should have been updated upon receiving the RPC
        case 0 =>
          val last = _log.last
          (_term.votedFor.isEmpty || _term.votedFor == Some(candidateId)) &&
            lastLogTerm >= last.term &&
            lastLogIndex >= last.index
        case 1 => false
      }
    }

    /** @return minimum number of nodes required for majority */
    private[this] def requiredMajority: Double =
      (_peers.size + 1.0) / 2.0

    /** Converts comma-separated host ports into a map.
      * @return map of `hostport`s to thrift clients
      */
    private[this] def extractPeers(prop: String): Map[String, RaftConsensusService[Future]] = {
      (prop split ",").map {
        case id =>
          val hostport = id.trim
          val client = Thrift.newIface[FutureIface](hostport)
          (hostport, client)
      }.toMap - _id
    }
  }

}
