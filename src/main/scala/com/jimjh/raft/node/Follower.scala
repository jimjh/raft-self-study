package com.jimjh.raft.node

import com.jimjh.raft.log.LogComponent
import com.jimjh.raft.node.State._
import com.jimjh.raft.rpc.RaftConsensusService.FutureIface
import com.jimjh.raft.rpc.{Vote, Entry}
import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory

/** Responds to requests for votes and replicates log entries.
  *
  * @author Jim Lim - jim@jimjh.com
  */
class Follower(override val id: String,
               override val term: Long,
               _log: LogComponent#Log,
               private[this] var _votedFor: Option[String] = None) extends Node {

  override val _logger = Logger(LoggerFactory getLogger s"Follower:$id")

  override val state = Fol

  override def votedFor = _votedFor

  override def start(peers: Map[String, FutureIface])(implicit machine: Machine) = this

  override def transition(to: State, newTerm: Long = term + 1) = {
    to match {
      case Fol =>
        require(newTerm > term)
        new Follower(id, newTerm, _log)
      case Cand =>
        require(newTerm >= term)
        new Candidate(id, newTerm, _log)
      case Ldr =>
        throw new IllegalArgumentException("Refusing to transition from follower to leader.")
    }
  }

  override def requestVote(reqTerm: Long,
                           candidateId: String,
                           lastLogIndex: Long,
                           lastLogTerm: Long)
                          (implicit machine: Machine) = {
    reqTerm compare term match {
      case -1 => // stale
        Vote(reqTerm, granted = false)
      case 0 =>
        _log.synchronized {
          val granted = shouldVote(candidateId, lastLogIndex, lastLogTerm)
          if (granted) _votedFor = Some(candidateId) // relies on _log for synchronization
          Vote(term, granted)
        }
      case 1 => // new leader
        machine
          .become(Fol, reqTerm)
          .requestVote(reqTerm, candidateId, lastLogIndex, lastLogTerm)
    }
  }

  override def appendEntries(reqTerm: Long,
                             leaderId: String,
                             prevLogIndex: Long,
                             prevLogTerm: Long,
                             entries: Seq[Entry],
                             leaderCommit: Long)
                            (implicit machine: Machine) = {
    reqTerm compare term match {
      case -1 => false // stale
      case 0 =>
        _log.commit = leaderCommit
        entries.isEmpty || replicateEntries(term, prevLogIndex, prevLogTerm, entries)
      case 1 => // new leader
        machine
          .become(Fol, reqTerm)
          .appendEntries(reqTerm, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit)
    }
  }

  override def timeout(implicit machine: Machine) = {
    _logger.info(s"Election timeout triggered. Current state is $state for term $term.")
    machine.become(Cand, term + 1)
  }

  /** @return true iff the given request deserves a vote */
  private[this] def shouldVote(candidateId: String,
                               lastLogIndex: Long,
                               lastLogTerm: Long): Boolean = {
    val last = _log.last
    votedFor.fold(true)(_ == candidateId) &&
      lastLogTerm >= last.term &&
      lastLogIndex >= last.index
  }

  private[this] def replicateEntries(term: Long,
                                     prevLogIndex: Long,
                                     prevLogTerm: Long,
                                     entries: Seq[Entry]): Boolean = _log.synchronized {
    // walk backwards until log index is found
    _log.findLast(prevLogIndex, prevLogTerm).fold(false) {
      root =>
        _logger.debug(s"Replicating log entries for term $term with prevIndex $prevLogIndex")
        _log.appendEntries(term, entries, root)
        true
    }
  }
}