package com.jimjh.raft

import com.jimjh.raft.rpc.RaftConsensusService.FutureIface
import com.jimjh.raft.rpc.{Entry, Vote}
import com.twitter.util.Future
import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory

/** Local proxy object for wrapping calls to remote nodes.
  *
  * The proxy is responsible for
  *
  * - sending heartbeats
  * - forwarding requests
  * - keeping track of next index, match index
  */
class Proxy(_id: String,
            _newClient: String => FutureIface)
  extends HeartBeatDelegate {

  // TODO initialize nextIndex, matchIndex
  // TODO reset heartbeats if append entries were sent

  private[this] val _logger = Logger(LoggerFactory getLogger "Proxy")
  private[this] val _client = _newClient(_id)
  private[this] var _heartBeat = Option.empty[HeartBeat]

  private[this] var _term = Option.empty[Term]

  /** Triggered by [[HeartBeat]]. Sends an empty AppendEntries RPC to each node. */
  override protected[raft] def pulse(term: Long) {
    // TODO send proper values
    _client.appendEntries(term, _id, -1, -1, Nil, -1)
  }

  /** Starts [[_heartBeat]]. Any existing heartbeats are canceled.
    *
    * @param term term for each the owner holds leadership
    */
  protected[raft] def startHeartBeat(term: Long) = synchronized {
    stopHeartBeat()
    _heartBeat = Some(new HeartBeat(this, term).start())
  }

  /** Stops [[_heartBeat]], if any. */
  protected[raft] def stopHeartBeat() = synchronized {
    _heartBeat map (_.cancel())
    _heartBeat = None
  }

  /** Release leadership. */
  protected[raft] def releaseTerm() = synchronized {
    _term = None
  }

  /** Acquire leadership. */
  protected[raft] def acquireTerm(num: Long, lastIndex: LogIndex) = synchronized {
    _term = Some(new Term(num, lastIndex, 0))
  }

  protected[raft] def requestVote(term: Long,
                                  candidateId: String,
                                  lastLogIndex: Long,
                                  lastLogTerm: Long): Future[Vote] = {
    _client
      .requestVote(term, candidateId, lastLogIndex, lastLogTerm)
      .onFailure(_logger.error(s"RequestVote failure.", _))
  }

  protected[raft] def appendEntries(term: Long,
                                    leaderId: String,
                                    prevLogIndex: Long,
                                    prevLogTerm: Long,
                                    entries: Seq[Entry],
                                    leaderCommit: Long): Future[Boolean] = {
    _client
      .appendEntries(term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit)
      .onFailure(_logger.error(s"AppendEntries failure.", _))
  }

  /** Sends an AppendEntries RPC to this node. */
  protected[raft] def sync(commitIndex: Long) = synchronized {
    _term.map { term =>
      val prevIndex = term.prevIndex
      val nextIndex = prevIndex.next // FIXME lock? check for tail?
    val entry = Entry(nextIndex.elem.cmd, nextIndex.elem.args)
      appendEntries(term.num, _id, prevIndex.index, prevIndex.term, List(entry), commitIndex)
        .onSuccess(afterSync(term.num, nextIndex, _))
    }
  }

  /** Adds the given append response to the current tally. */
  private[this] def afterSync(num: Long, index: LogIndex, accepted: Boolean) = synchronized {
    accepted match {
      case true =>
        _term.map(_.advanceIndices(num, index))
      // TODO update commit index, apply log
      case false =>
        _term.map(_.retreatIndices(num, index))
      // TODO resend RPC
    }
  }

  /**
   * @param prevIndex index just before nextIndex
   * @param matchIndex index of the highest log entry known to be replicated
   */
  class Term(val num: Long,
             var prevIndex: LogIndex,
             var matchIndex: Long) {

    /** Updates both `prevIndex` and `matchIndex`.
      *
      * @param reqNum    term number of the AppendEntries request
      * @param nextIndex index of the last log entry accepted
      */
    def advanceIndices(reqNum: Long, nextIndex: LogIndex) {
      if (reqNum == num) {
        prevIndex = List(prevIndex, nextIndex).max
        matchIndex = List(matchIndex, nextIndex.index).max
      }
    }

    /** Updates `prevIndex` to decrement it.
      *
      * @param reqNum     term number of the AppendEntries request
      * @param nextIndex  index of the last log entry rejected
      */
    def retreatIndices(reqNum: Long, nextIndex: LogIndex) {
      if (reqNum == num) {
        prevIndex = List(prevIndex, nextIndex.prev).min
      }
    }
  }

}
