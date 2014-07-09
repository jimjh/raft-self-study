package com.jimjh.raft.node

import com.jimjh.raft.log.LogEntry
import com.jimjh.raft.rpc.Entry
import com.jimjh.raft.rpc.RaftConsensusService.FutureIface
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
            _client: FutureIface,
            _leader: Leader,
            _last: LogEntry)
  extends HeartBeatDelegate {

  // TODO reset heartbeats if append entries were sent
  // TODO use a different log interface
  // TODO verify that finagle-thrift has a built-in retries w. exponential back-off
  // TODO cancel retries for heartbeats

  private[this] val _logger = Logger(LoggerFactory getLogger "Proxy")
  private[this] var _heartBeat = Option.empty[HeartBeat]

  @volatile
  private[this] var stopped: Boolean = false

  @volatile
  private[this] var prevIndex: LogEntry = _last

  @volatile
  private[this] var matchIndex: Long = 0L

  startHeartBeat()
  sync()

  /** Triggered by [[HeartBeat]]. Sends an empty AppendEntries RPC to each node. */
  override def pulse(term: Long) {
    val entry = prevIndex
    _client.appendEntries(term, _leader.id, entry.index, entry.term, Nil, _leader.commit)
  }

  /** Stop sending heartbeats or replicating log entries. */
  def stop() = {
    stopHeartBeat()
    stopped = true
  }

  def replicate(prevLogIndex: Long,
                prevLogTerm: Long,
                entries: Seq[Entry]): Future[Boolean] = {
    _client
      .appendEntries(
        _leader.term,
        _leader.id,
        prevLogIndex,
        prevLogTerm,
        entries,
        _leader.commit)
  }

  private[this] def sync(): Unit = {
    val nextIndex = prevIndex.nextF
    nextIndex.map { next =>
      val entry = Entry(next.cmd, next.args)
      if (!stopped)
        replicate(prevIndex.index, prevIndex.term, List(entry))
          .onSuccess(onSuccessfulAppend(next, _))
          .onFailure(e => _logger.warn(s"Failed to sync after log index ${prevIndex.index}.", e))
    }
  }

  /** Adds the given append response to the current tally.
    *
    * @param index index of the log entry that was sent
    */
  private[this] def onSuccessfulAppend(index: LogEntry,
                                       accepted: Boolean) = {
    if (!stopped) {
      accepted match {
        case true =>
          advanceIndices(index)
          _leader.updateMatch(_id, matchIndex)
        case false =>
          retreatIndices(index)
      }
      sync()
    }
  }

  /** Starts [[_heartBeat]]. Any existing heartbeats are canceled. */
  private[this] def startHeartBeat() = {
    _heartBeat = Some(new HeartBeat(this, _leader.term).start())
  }

  /** Stops [[_heartBeat]], if any. */
  private[this] def stopHeartBeat() = {
    _heartBeat map { _.cancel() }
    _heartBeat = None
  }

  /** Creates a copy with [[prevIndex]] and [[matchIndex]] advanced.
    *
    * @param nextIndex index of the last log entry
    */
  private[this] def advanceIndices(nextIndex: LogEntry) = {
    prevIndex = List(prevIndex, nextIndex).max
    matchIndex = List(matchIndex, nextIndex.index).max
  }

  /** Creates a copy with [[prevIndex]] retreated.
    *
    * @param nextIndex  index of the last log entry
    */
  private[this] def retreatIndices(nextIndex: LogEntry) = {
    prevIndex = List(prevIndex, nextIndex.prev).min
  }
}
