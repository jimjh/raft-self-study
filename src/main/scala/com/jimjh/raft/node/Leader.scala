package com.jimjh.raft.node

import com.jimjh.raft.log.LogComponent
import com.jimjh.raft.node.State._
import com.jimjh.raft.rpc.RaftConsensusService.FutureIface
import com.jimjh.raft.rpc.{Entry, Vote}
import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.collection.mutable

/** Replicates logs to followers.
  *
  * @author Jim Lim - jim@jimjh.com
  */
class Leader(override val id: String,
             override val term: Long,
             _log: LogComponent#Log) extends Node {

  override val _logger = Logger(LoggerFactory getLogger s"Leader:$id")

  override val state = Ldr

  /** Ordered set of match indexes for each peer, stored as `(id, match index)`. */
  private[this] val _matches = new mutable.TreeSet[Match]()

  private[this] var _size = 0
  private[this] var _proxies = Map.empty[String, Proxy]

  override def votedFor: Option[String] = None

  /** @param reqTerm request term, assumed to be at most [[term]]*/
  override def appendEntries(reqTerm: Long,
                             leaderId: String,
                             prevLogIndex: Long,
                             prevLogTerm: Long,
                             entries: Seq[Entry],
                             leaderCommit: Long)
                            (implicit machine: Machine) = {
    reqTerm compare term match {
      case -1 | 0 =>
        _logger.warn(s"Ignoring AppendEntries RPC, since I am the leader for term $term")
        false
      case 1 =>
        machine
          .become(Fol, reqTerm)
          .appendEntries(reqTerm, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit)
    }
  }

  /** @param reqTerm request term, assumed to be at most [[term]]*/
  override def requestVote(reqTerm: Long,
                           candidateId: String,
                           lastLogIndex: Long,
                           lastLogTerm: Long)
                          (implicit machine: Machine) = {
    reqTerm compare term match {
      case -1 | 0 => // stale, and candidates already voted for themselves
        Vote(reqTerm, granted = false)
      case 1 => // new election
        machine
          .become(Fol, reqTerm)
          .requestVote(reqTerm, candidateId, lastLogIndex, lastLogTerm)
    }
  }

  override def start(peers: Map[String, FutureIface])
                    (implicit machine: Machine): Node = {
    _size = peers.size
    val last = _log.last
    _proxies = peers.map {
      case (peer, client) => (peer, new Proxy(peer, client, this, last))
    }.toMap
    this
  }

  /** Creates the next node without mutating the state of this node.
    *
    * @return node
    */
  override def transition(to: State, newTerm: Long): Node = {
    to match {
      case Fol =>
        require(newTerm > term)
        stop()
        new Follower(id, newTerm, _log)
      case Cand =>
        require(newTerm > term)
        stop()
        new Candidate(id, newTerm, _log)
      case Ldr =>
        throw new IllegalArgumentException("Refusing to transition from leader to leader.")
    }
  }

  override def timeout(implicit machine: Machine) = {
    _logger.warn("This is odd. Leaders shouldn't get timeouts.")
  }

  /** Tells the leader that a peer's match index has been advanced.
    *
    * @param id ID of the node this proxy represents
    * @param index new match index
    */
  def updateMatch(id: String, index: Long) = _log.synchronized {
    _logger.trace(s"Updating match index for $id to $index")

    // update match index for given peer
    _matches.find(_.id == id) match {
      case Some(p) =>
        _matches.remove(p)
      case None =>
    }
    _matches.add(Match(id, index))
    // minus one for the leader
    val req = majority(_size) - 1
    // advance commit
    val fastest = _matches.takeRight(req)
    if (fastest.size >= req) _log.commit = fastest.firstKey.index
  }

  def commit = _log.commit

  private[this] def stop() = synchronized {
    _proxies.map {
      case (_, proxy) => proxy.stop()
    }
  }

  case class Match(id: String, index: Long) extends Ordered[Match] {
    override def compare(other: Match) = index compareTo other.index
  }
}
