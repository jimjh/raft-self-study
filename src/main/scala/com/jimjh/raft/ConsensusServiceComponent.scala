package com.jimjh.raft

import java.util.Properties

import com.jimjh.raft.log.LogComponent
import com.jimjh.raft.node.State._
import com.jimjh.raft.node.{Machine, Node, State}
import com.jimjh.raft.rpc.RaftConsensusService.FutureIface
import com.jimjh.raft.rpc.{Entry, RaftConsensusService, Vote}
import com.twitter.finagle.Thrift
import com.twitter.util.{Future, Promise, Try}
import com.typesafe.scalalogging.slf4j.Logger

import scala.concurrent.promise

/** Wrapper for a Consensus Service.
  *
  * It looks odd because I am trying to use the Cake Pattern.
  *
  * @author Jim Lim - jim@jimjh.com
  */
trait ConsensusServiceComponent {

  /** Manages RPCs implementing the RAFT consensus algorithm */
  val consensus: ConsensusService // #injectable

  /** Serializable array of log entries. */
  val log: LogComponent#Log // #injected

  /** Serializes and de-serializes tuples. */
  val persistence: PersistenceComponent#Persistence // #injected

  /** Restartable, stoppable election timer. */
  protected val timer: ElectionTimerComponent#ElectionTimer // #injected

  /** slf4j logger. */
  protected[raft] val logger: Logger // #injected

  /** @return true iff this server is currently a leader */
  def isLeader: Boolean = consensus.isLeader

  /** @return commit index */
  def commit: Long = log.commit

  /** Responds to RPCs from other servers using the RAFT algorithm.
    *
    * Each server is implemented as a state machine that governs the transitions between
    * `FOLLOWER`, `CANDIDATE`, and `LEADER`. The `host:port` of each node will be treated as its
    * node ID _i.e._ each node is expected be restarted with the same `host:port`.
    *
    * @note The timer parameter is a bit funky. The constructor needs an [[ElectionTimerComponent.ElectionTimer]]
    *       instance, whose constructor needs an [[Timeoutable]] instance that is, usually, `this`. To work'
    *       around this problem, I let `timer` be a call-by-name parameter, which will be called exactly once to
    *       instantiate a private lazy val `timer`. Refer to [[RaftServer]] for an example.
    *
    * @param _props configuration options for the consensus service
    * @param _newClient factory function that returns a new Thrift RPC client
    */
  class ConsensusService(_props: Properties,
                         _newClient: String => FutureIface)
    extends RaftConsensusService[Future]
    with Timeoutable
    with Machine {

    notNull(_props, "_props")
    notNull(_newClient, "_newClient")

    /** Node ID */
    val id = _props getProperty "node.id"

    /** Map of node IDs to thrift clients. */
    private[this] val _peers = extractPeers(_props getProperty "peers")

    @volatile
    private[this] var _node: Node = null

    /* BEGIN RPC API **/

    /** Invoked by candidates to gather votes.
      *
      * @return vote
      */
    override def requestVote(term: Long,
                             candidateId: String,
                             lastLogIndex: Long,
                             lastLogTerm: Long): Future[Vote] = {
      logger.debug(s"Received RequestVote($term, $candidateId, $lastLogIndex, $lastLogTerm)")
      new Promise(Try {
        val vote = _node.requestVote(term, candidateId, lastLogIndex, lastLogTerm)
        if (vote.granted) {
          // avoid starting an election if one is in progress (optional)
          timer.restart()
          persist()
        }
        vote
      })
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
      logger.trace(s"Received AppendEntries($term, $leaderId, $prevLogIndex, $prevLogTerm)")
      if (term >= _node.term) timer.restart()
      new Promise(Try {
        _node.appendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit)
      })
    }

    /* END RPC API */

    /** Initializes the consensus service. */
    def start() = {
      // recover from disk, or start with 0
      _node = recover.getOrElse(new node.Follower(id, 0, log)).start(_peers)

      // start thrift server
      val server = Thrift.serveIface(id, this)
      logger.info(s"Starting consensus service - ${_props}")

      timer.restart()
      persist()
      server
    }

    def apply(cmd: String, args: Seq[String]) = {
      val p = promise[ReturnType]()
      log.append(_node.term, cmd, args, Some(p))
      p
    }

    def isLeader: Boolean = _node.state == Ldr

    /* Triggered by [[ElectionTimerComponent.ElectionTimer]]. */
    override def timeout() = _node.timeout

    /* Triggered by [[com.jimjh.raft.node.Node]]. */
    override def become(to: State.State, term: Long)
                       (implicit node: Node) = synchronized {
      if (_node eq node) {
        // ignore stale requests
        logger.info(s"(state: ${_node.state}, term: ${_node.term}) ~> (state: $to, term: $term)")
        to match {
          case Fol | Cand =>
            timer.restart()
            _node = _node.transition(to, term).start(_peers)
          case Ldr =>
            timer.cancel()
            _node = _node.transition(to, term).start(_peers)
        }
        persist()
      }
      _node
    }

    /** Sets `_node` to contents from saved state, discarding any existing nodes. */
    private[this] def recover: Option[Node] = {
      persistence.readNode[(Long, Option[String])] match {
        case Some((term, vote)) =>
          Some(new node.Follower(id, term, log, vote))
        case None =>
          None
      }
    }

    private[this] def persist() {
      logger.debug(s"PERSIST term: ${_node.term}, votedFor: ${_node.votedFor}")
      persistence.writeNode((_node.term, _node.votedFor))
    }

    /** Converts comma-separated host ports into a map.
      * @return map of `hostport`s to thrift clients
      */
    private[this] def extractPeers(prop: String): Map[String, FutureIface] = {
      (prop split ",").map {
        case peer =>
          val hostport = peer.trim
          (hostport, _newClient(hostport))
      }.toMap - id
    }
  }
}
