package com.jimjh.raft.node

import com.jimjh.raft.log.LogComponent
import com.jimjh.raft.node.State._
import com.jimjh.raft.rpc.RaftConsensusService.FutureIface
import com.jimjh.raft.rpc.{Entry, Vote}
import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._

/** Starts elections and tries to become the leader.
  *
  * @author Jim Lim - jim@jimjh.com
  */
class Candidate(override val id: String,
                override val term: Long,
                _log: LogComponent#Log) extends Node {

  override val _logger = Logger(LoggerFactory getLogger s"Candidate:$id")

  override val state = Candidate

  override def votedFor = Some(id)

  override def start(peers: Map[String, FutureIface])
                    (implicit machine: Machine) = {
    _logger.info(s"Starting an election for term $term.")
    val last = _log.last
    requestVotes(peers, term, last.term, last.index)
    this
  }

  override def transition(to: State, newTerm: Long) = {
    to match {
      case Follower =>
        require(newTerm >= term)
        new Follower(id, newTerm, _log)
      case Candidate =>
        require(newTerm > term)
        new Candidate(id, newTerm, _log)
      case Leader =>
        require(newTerm >= term)
        new Leader(id, newTerm, _log)
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
      case -1 => // stale
        false
      case 0 | 1 => // new leader
        machine
          .become(Follower, reqTerm)
          .appendEntries(reqTerm, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit)
    }
  }

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
          .become(Follower, reqTerm)
          .requestVote(reqTerm, candidateId, lastLogIndex, lastLogTerm)
    }
  }

  /** Sends a RequestVote RPC to each node. */
  private[this] def requestVotes(peers: Map[String, FutureIface],
                                 term: Long,
                                 logTerm: Long,
                                 logIndex: Long)
                                (implicit machine: Machine) {
    val req = majority(peers.size)
    val votes = TrieMap[String, Boolean]((id, true))
    peers.foreach {
      case (peer, proxy) =>
        _logger.debug(s"Sending RequestVote RPC to $peer")
        future {
          // wrapping it in a future helps to send out requests in quick succession
          proxy
            .requestVote(term, id, logTerm, logIndex)
            .onSuccess(tallyVotes(votes, req, peer, _))
        }
    }
  }

  override def timeout(implicit machine: Machine) {
    _logger.info(s"Election timeout triggered. Current state is $state for term $term.")
    machine.become(Candidate, term + 1)
  }

  /** Adds the given vote to the current tally.
    *
    * @param id   voter ID
    * @param vote (`term`, `granted`)
    */
  private[this] def tallyVotes(votes: TrieMap[String, Boolean],
                               req: Long,
                               id: String,
                               vote: Vote)
                              (implicit machine: Machine) {
    _logger.debug(s"Vote received from $id: $vote")
    if (vote == Vote(term, granted = true)) {
      votes += ((id, true))
      // TODO test that this doesn't get called multiple times
      if (votes.size >= req) machine.become(Leader, term)
    }
  }
}
