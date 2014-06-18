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
  class ConsensusService(private[this] val _props: Properties,
                         private[this] val _log: LogComponent#Log,
                         private[this] val timer: => ElectionTimerComponent#ElectionTimer, // #funky
                         private[this] val _newClient: String => FutureIface)
    extends RaftConsensusService[Future]
    with ElectionTimerDelegate
    with HeartBeatDelegate {

    notNull(_props, "_props")
    notNull(_log, "_log")
    notNull(_newClient, "_newClient")

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
    private[this] val _id = _props getProperty "node.id"

    private[this] var _state = Follower
    private[this] val _logger = Logger(LoggerFactory getLogger s"ConsensusService:${_id}")
    private[this] var _heartBeat = Option.empty[HeartBeat]
    private[this] lazy val _timer = timer // #funky

    /** Map of node IDs to thrift clients */
    private[this] val _peers = extractPeers(_props getProperty "peers")

    /** Set of peer IDs that voted for this candidate */
    private[this] var _votesReceived = Set.empty[String]

    // TODO persistent state
    private[this] var _term = new Term(0, None)

    /* TODO keep peer indices
     * On positive response, (if leader) update index w. max of existing value and new value.
     * On negative response, (if leader) update index w. min of existing value and new value.
     * Wrap "appendEntryToPeer" for use by both #pulse and #apply.
     * Each "peer" should have a host:port and an atomic index.
     *
     * TODO keep track of log replication
     * When an index is increased, check for opportunities to raise commit index.
     *   Say, required_majority is k. Order indices in descending order, find kth index. This is the commit index.
     * If commit index is raised, apply additional log entries to the log.
     * Get a response back to the client.
     *
     * TODO implement
     * on increase index
     *   if can raise commit index
     *     raise commit index
     *     applyUntil(commitIndex)
     *
     * class AtomicIndex
     *   def setAtLeast(n): Boolean
     *   def setAtMost(n): Boolean
     *   def get
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
      _log.append(_term.term, cmd, args, Some(promise))
      // TODO kick off a bunch of AppendEntries RPCs, so we don't depend on heartbeats
      // TODO no need to send heartbeats if we recently sent a legit AppendEntries
      promise
    }

    /** Triggered by [[ElectionTimerComponent.ElectionTimer]]. */
    override protected[raft] def timeout() = synchronized {
      _logger.info(s"Election timeout triggered. Current state is $state for term ${_term}.")
      state match {
        case Follower | Candidate => becomeCandidate()
      }
    }

    /** Triggered by [[HeartBeat]]. Sends an empty AppendEntries RPC to each node. */
    override protected[raft] def pulse(term: Long) {
      _peers.values.foreach(_.appendEntries(term, _id, -1, -1, Nil, -1))
    }

    /** Sends a RequestVote RPC to each node. */
    private[this] def requestVotes(term: Long, logTerm: Long, logIndex: Long) {
      _peers.foreach {
        case (id, client) =>
          _logger.debug(s"Sending RequestVote RPC to $id")
          future {
            // wrapping it in a future helps to send out requests in quick succession
            client.requestVote(term, _id, logTerm, logIndex)
              .onSuccess(tallyVotes(id, _))
              .onFailure(_logger.error(s"RequestVote failure.", _))
            // TODO verify that finagle-thrift has a built-in retries w. exponential back-off
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
        if (_state == Candidate && vote == Vote(_term.term, granted = true)) {
          _votesReceived += id
          if (_votesReceived.size >= requiredMajority) becomeLeader()
        }
      }
    }

    /** Starts [[_heartBeat]].
      *
      * @param term term for each this node holds leadership
      */
    private[this] def startHeartBeat(term: Long) = synchronized {
      _heartBeat = Some(new HeartBeat(this, term).start())
    }

    private[this] def stopHeartBeat() = synchronized {
      _heartBeat map (_.cancel())
      _heartBeat = None
    }

    /** Sets state to [[Leader]]. */
    private[this] def becomeLeader() = synchronized {
      _logger.info(s"(state: ${_state}, term: ${_term}) ~> (state: Leader, term: ${_term})")

      _state = Leader
      startHeartBeat(_term.term)
      _timer.cancel()
    }

    /** Starts an election and restarts the [[ElectionTimerComponent.ElectionTimer]]. */
    private[this] def becomeCandidate() = synchronized {
      _state = Candidate
      _term = new Term(_term.term + 1, Some(_id))
      _logger.info(s"Starting an election for term ${_term}.")

      _votesReceived = Set(_id)

      val last = _log.last
      requestVotes(_term.term, last.term, last.index)

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
      _term.term == term match {
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
      val last = _log.last
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
    private[this] def extractPeers(prop: String): Map[String, RaftConsensusService[Future]] = {
      (prop split ",").map {
        case id =>
          val hostport = id.trim
          val client = _newClient(hostport)
          (hostport, client)
      }.toMap - _id
    }

    private[this] def process[T](term: Long, resp: T)(f: => T): Promise[T] = {
      new Promise(Try {
        synchronized {
          term < _term.term match {
            case true => resp
            case false => f
          }
        }
      })
    }
  }

}
