package com.jimjh.raft

import java.util.Properties

import com.jimjh.raft.rpc.RaftConsensusService.FutureIface
import com.jimjh.raft.rpc.{Entry, RaftConsensusService, Vote}
import com.twitter.util.{Future, Promise, Try}
import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.future

/** Wrapper for a Consensus Service.
  *
  * It looks odd because I am trying to use the Cake Pattern.
  *
  * @author Jim Lim - jim@jimjh.com
  */
trait ConsensusServiceComponent {

  type Node = RaftConsensusService[Future]
  val consensusService: ConsensusService

  /** Responds to RPCs from other servers using the RAFT algorithm.
    *
    * Each server is implemented as a state machine that governs the transitions between
    * `FOLLOWER`, `CANDIDATE`, and `LEADER`. The `host:port` of each node will be treated as its
    * node ID _i.e._ each node is expected be restarted with the same `host:port`.
    *
    * @note The timer parameter is a bit funky. The constructor needs an [[ElectionTimerComponent.ElectionTimer]]
    *       instance, whose constructor needs an [[ElectionTimerDelegate]] instance that is, usually, `this`. To work'
    *       around this problem, I let `timer` be a call-by-name parameter, which will be called exactly once to
    *       instantiate a private lazy val `_timer`. Refer to [[RaftServer]] for an example.
    *
    * @param _props configuration options for the consensus service
    * @param timer provider function that returns an [[ElectionTimerComponent.ElectionTimer]] instance
    * @param _newClient factory function that returns a new Thrift RPC client
    */
  class ConsensusService(_props: Properties,
                         _log: LogComponent#Log,
                         timer: => ElectionTimerComponent#ElectionTimer, // #funky
                         _newClient: String => FutureIface)
    extends Node
    with ElectionTimerDelegate {

    notNull(_props, "_props")
    notNull(_log, "_log")
    notNull(_newClient, "_newClient")

    /** Defines various possible states for the raft server. */
    object State extends Enumeration {
      type State = Value
      val Follower, Candidate, Leader = Value
    }

    import State._

    class Term(val num: Long, var votedFor: Option[String]) {
      def <=(other: Long) = num <= other

      def <(other: Long) = num < other

      def compare(other: Long) = num.compare(other)

      override def toString = s"$num"
    }

    /** Node ID */
    private[this] val _id = _props getProperty "node.id"

    private[this] var _state = Follower
    private[this] val _logger = Logger(LoggerFactory getLogger s"ConsensusService:${_id}")
    private[this] lazy val _timer = timer // #funky

    /** Map of node IDs to thrift clients. */
    private[this] val _peers = extractPeers(_props getProperty "peers")

    /** Set of peer IDs that voted for this candidate. */
    private[this] var _votesReceived = Set.empty[String]

    // TODO persistent state
    private[this] var _term = new Term(0, None)

    private[this] var _commit = 0

    /* TODO read/write locks?
     * TODO verify that finagle-thrift has a built-in retries w. exponential back-off
     * TODO cancel retries for heartbeats
     * TODO use local proxy objects for dealing w. heartbeat resets, prev index, match index?
     */

    def state: State = _state

    /* BEGIN RPC API **/

    /** Invoked by candidates to gather votes.
      *
      * @return vote
      */
    override def requestVote(term: Long,
                             candidateId: String,
                             lastLogIndex: Long,
                             lastLogTerm: Long): Future[Vote] = {
      _logger.debug(s"Received RequestVote($term, $candidateId, $lastLogIndex, $lastLogTerm)")
      process(term, Vote(term, granted = false)) {
        if (_term < term) becomeFollower(term)
        val granted = shouldVote(term, candidateId, lastLogIndex, lastLogTerm)
        if (granted) {
          _term.votedFor = Some(candidateId)
          _timer.restart() // avoid starting an election if one is in progress (optional)
        }
        Vote(term, granted)
      }
    }

    /** Invoked by leader to replicate log entries; also acts as heartbeat.
      *
      * @return true iff accepted
      */
    override def appendEntries(term: Long,
                               leaderId: String,
                               prevLogIndex: Long,
                               prevLogTerm: Long,
                               entries: Seq[Entry],
                               leaderCommit: Long): Future[Boolean] = {
      _logger.trace(s"Received AppendEntries($term, $leaderId, $prevLogIndex, $prevLogTerm)")
      process(term, false) {
        state match {
          case Follower => updateFollower(term)
          case Candidate => updateCandidate(term)
          case Leader => updateLeader(term)
        }
      }
    }

    /* END RPC API */

    /** Initializes the consensus service. */
    def start() = {
      _logger.info(s"Starting consensus service - ${_props}")
      _timer.restart()
    }

    def apply(cmd: String, args: Array[String]) = {
      val promise = new Promise[ReturnType]()
      _log.append(_term.num, cmd, args, Some(promise))
      // TODO kick off a bunch of AppendEntries RPCs
      promise
    }

    /** Triggered by [[ElectionTimerComponent.ElectionTimer]]. */
    override protected[raft] def timeout() = synchronized {
      _logger.info(s"Election timeout triggered. Current state is $state for term ${_term}.")
      state match {
        case Follower | Candidate => becomeCandidate()
      }
    }

    /** Sends a AppendEntries RPC to each node. */
    private[this] def appendEntriesToPeers() = synchronized {
      val term = _term.num
      _peers.values.foreach { node => future {
        node.sync(_commit)
      }
      }
    }

    /** Sends a RequestVote RPC to each node. */
    private[this] def requestVotes(term: Long, logTerm: Long, logIndex: Long) {
      _peers.foreach {
        case (id, node) =>
          _logger.debug(s"Sending RequestVote RPC to $id")
          future {
            // wrapping it in a future helps to send out requests in quick succession
            node.requestVote(term, _id, logTerm, logIndex)
              .onSuccess(tallyVotes(id, _))
          }
      }
    }

    /** Adds the given vote to the current tally.
      *
      * @param id   voter ID
      * @param vote (`term`, `granted`)
      */
    private[this] def tallyVotes(id: String, vote: Vote) {
      _logger.debug(s"RequestVote success from $id: $vote")
      synchronized {
        if (_state == Candidate && vote == Vote(_term.num, granted = true)) {
          _votesReceived += id
          if (_votesReceived.size >= requiredMajority) becomeLeader()
        }
      }
    }

    /** @param term term for each this node holds leadership */
    private[this] def startHeartBeat(term: Long) = synchronized {
      _peers.values.foreach(_.startHeartBeat(term))
    }

    private[this] def stopHeartBeat() = synchronized {
      _peers.values.foreach(_.stopHeartBeat())
    }

    /** Sets state to [[Leader]]. */
    private[this] def becomeLeader() = synchronized {
      _logger.info(s"(state: ${_state}, term: ${_term}) ~> (state: Leader, term: ${_term})")

      _state = Leader
      startHeartBeat(_term.num)
      _timer.cancel()
    }

    /** Starts an election and restarts the [[ElectionTimerComponent.ElectionTimer]]. */
    private[this] def becomeCandidate() = synchronized {
      _state = Candidate
      _term = new Term(_term.num + 1, Some(_id))
      _logger.info(s"Starting an election for term ${_term}.")

      _votesReceived = Set(_id)

      val last = _log.lastEntry
      requestVotes(_term.num, last.term, last.index)

      _timer.restart()
    }

    /** Sets state to [[Follower]], resetting the term if necessary. */
    private[this] def becomeFollower(term: Long) = synchronized {
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

    private[this] def updateLeader(term: Long): Boolean = synchronized {
      _term.num == term match {
        case true =>
          _logger.warn(s"Ignoring AppendEntries RPC, since I am the leader for term $term")
          false
        case false =>
          becomeFollower(term)
          updateFollower(term)
      }
    }

    private[this] def updateCandidate(term: Long): Boolean = synchronized {
      becomeFollower(term)
      updateFollower(term)
    }

    private[this] def updateFollower(term: Long): Boolean = synchronized {
      _timer.restart()
      true
    }

    /** @return true iff the given request deserves a vote */
    private[this] def shouldVote(term: Long,
                                 candidateId: String,
                                 lastLogIndex: Long,
                                 lastLogTerm: Long): Boolean = synchronized {
      val last = _log.lastEntry
      (_term.votedFor.isEmpty || _term.votedFor == Some(candidateId)) &&
        lastLogTerm >= last.term &&
        lastLogIndex >= last.index
    }

    /** @return minimum number of nodes required for majority */
    private[this] def requiredMajority: Double =
      (_peers.size + 1.0) / 2.0

    /** Converts comma-separated host ports into a map.
      * @return map of `hostport`s to thrift clients
      */
    private[this] def extractPeers(prop: String): Map[String, Proxy] = {
      (prop split ",").map {
        case id =>
          val hostport = id.trim
          val client = new Proxy(hostport, _newClient)
          (hostport, client)
      }.toMap - _id
    }

    private[this] def process[T](term: Long, resp: T)(f: => T): Promise[T] = {
      new Promise(Try {
        synchronized {
          // get a lock on the server state
          term < _term.num match {
            case true => resp
            case false => f // reject requests from stale peers
          }
        }
      })
    }
  }

}
