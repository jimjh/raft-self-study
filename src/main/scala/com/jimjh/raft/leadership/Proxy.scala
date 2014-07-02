package com.jimjh.raft.leadership

import com.jimjh.raft.log.LogEntry
import com.jimjh.raft.rpc.RaftConsensusService.FutureIface
import com.jimjh.raft.rpc.{Entry, Vote}
import com.jimjh.raft.{HeartBeat, HeartBeatDelegate}
import com.twitter.util.Future
import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global

/** Local proxy object for wrapping calls to remote nodes.
  *
  * The proxy is responsible for
  *
  * - sending heartbeats
  * - forwarding requests
  * - keeping track of next index, match index
  */
class Proxy(_id: String,
            _client: FutureIface)
  extends HeartBeatDelegate {

  // TODO reset heartbeats if append entries were sent
  // TODO use a different log interface
  // TODO verify that finagle-thrift has a built-in retries w. exponential back-off
  // TODO cancel retries for heartbeats

  private[this] val _logger = Logger(LoggerFactory getLogger "Proxy")
  private[this] var _heartBeat = Option.empty[HeartBeat]
  private[this] var _term = Option.empty[Long]

  /** Triggered by [[HeartBeat]]. Sends an empty AppendEntries RPC to each node. */
  override def pulse(term: Long) {
    // TODO send proper values
    _client.appendEntries(term, _id, -1, -1, Nil, -1)
  }

  /** Acquire leadership. */
  def acquireTerm(num: Long, leader: Leader) = synchronized {
    _term = Some(num)
    startHeartBeat(num)
    sync(Term(num, leader, leader.lastLogEntry))
  }

  /** Release leadership. */
  def releaseTerm() = synchronized {
    _term = None
    stopHeartBeat()
  }

  def requestVote(term: Long,
                  candidateId: String,
                  lastLogIndex: Long,
                  lastLogTerm: Long): Future[Vote] = {
    _client
      .requestVote(term, candidateId, lastLogIndex, lastLogTerm)
      .onFailure(_logger.error(s"RequestVote failure.", _))
  }

  def appendEntries(term: Long,
                    leaderId: String,
                    prevLogIndex: Long,
                    prevLogTerm: Long,
                    entries: Seq[Entry],
                    leaderCommit: Long): Future[Boolean] = {
    _client
      .appendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit)
      .onFailure(_logger.error(s"AppendEntries failure.", _))
  }

  private[this] def sync(term: Term): Unit = synchronized {
    val prevIndex = term.prevIndex
    val nextIndex = prevIndex.nextF
    nextIndex.map { next =>
      val entry = Entry(next.cmd, next.args)
      val commit = term.leader.commit
      appendEntries(term.num, _id, prevIndex.index, prevIndex.term, List(entry), commit)
        .onSuccess(onSuccessfulAppend(term, next, _))
        .onFailure(e => _logger.warn(s"Failed to sync after log index ${prevIndex.index}.", e))
    }
  }

  /** Adds the given append response to the current tally.
    *
    * @param index index of the log entry that was sent
    */
  private[this] def onSuccessfulAppend(term: Term,
                                       index: LogEntry,
                                       accepted: Boolean) = synchronized {
    val t = accepted match {
      case true =>
        val t = term.advanceIndices(index)
        t.leader.updateMatch(t.num, _id, t.matchIndex)
        t
      case false =>
        term.retreatIndices(index)
    }
    _term.map { num => if (num == t.num) sync(t)}
  }

  /** Starts [[_heartBeat]]. Any existing heartbeats are canceled.
    *
    * @param term term for each the owner holds leadership
    */
  private[this] def startHeartBeat(term: Long) = synchronized {
    stopHeartBeat()
    _heartBeat = Some(new HeartBeat(this, term).start())
  }

  /** Stops [[_heartBeat]], if any. */
  private[this] def stopHeartBeat() = synchronized {
    _heartBeat map (_.cancel())
    _heartBeat = None
  }

  case class Term(num: Long,
                  leader: Leader,
                  prevIndex: LogEntry,
                  matchIndex: Long = 0L) {

    /** Creates a copy with [[prevIndex]] and [[matchIndex]] advanced.
      *
      * @param nextIndex index of the last log entry
      */
    def advanceIndices(nextIndex: LogEntry) = {
      val p = List(prevIndex, nextIndex).max
      val m = List(matchIndex, nextIndex.index).max
      Term(num, leader, p, m)
    }

    /** Creates a copy with [[prevIndex]] retreated.
      *
      * @param nextIndex  index of the last log entry
      */
    def retreatIndices(nextIndex: LogEntry) = {
      val p = List(prevIndex, nextIndex.prev).min
      Term(num, leader, p, matchIndex)
    }
  }

}
