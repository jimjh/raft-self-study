package com.jimjh.raft.node

import com.jimjh.raft.node.State._
import com.jimjh.raft.rpc.RaftConsensusService.FutureIface
import com.jimjh.raft.rpc.{Entry, Vote}
import com.typesafe.scalalogging.slf4j.Logger

trait Machine {
  implicit protected val machine = this
  def become(state: State, term: Long)(implicit node: Node): Node
}

/** Follower, Candidate, or Leader.
  *
  * @author Jim Lim - jim@jimjh.com
  */
trait Node {
  val id: String
  val term: Long
  val state: State
  implicit val node = this
  protected val _logger: Logger

  def votedFor: Option[String]

  def start(peers: Map[String, FutureIface])
           (implicit machine: Machine): Node

  /** Creates the next node without mutating the state of this node.
    *
    * @return node
    */
  def transition(to: State, term: Long): Node

  def requestVote(reqTerm: Long,
                  candidateId: String,
                  lastLogIndex: Long,
                  lastLogTerm: Long)
                 (implicit machine: Machine): Vote

  def appendEntries(reqTerm: Long,
                    leaderId: String,
                    prevLogIndex: Long,
                    prevLogTerm: Long,
                    entries: Seq[Entry],
                    leaderCommit: Long)
                   (implicit machine: Machine): Boolean

  def timeout(implicit machine: Machine)

  /**
    * @param size number of peers, excluding the current node
    * @return required majority
    */
  def majority(size: Long) = ((size + 1.0) / 2.0).ceil.toInt
}
