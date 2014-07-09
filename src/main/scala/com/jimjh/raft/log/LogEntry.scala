package com.jimjh.raft.log

import com.jimjh.raft._

import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps

/** Skeletal implementation of a linked list.
  *
  * Only nextF and nextP may be mutable. All other fields, including prev, are immutable. The start of the list is
  * called a _sentinel_, which is a special entry that points to itself.
  *
  * @todo TODO persistent state using append, flush
  * @todo TODO initialize from persisted state
  *
  * @param term term during which this entry was created
  * @param index log index
  * @param result optional promise that will be fulfilled after `cmd` is applied
  * @param _prev previous log entry
  */
class LogEntry(val term: Long,
               val index: Long,
               val cmd: String,
               val args: Seq[String] = Array.empty[String],
               val result: Option[Promise[ReturnType]] = None,
               private[this] var _prev: LogEntry = LogEntry.sentinel)
  extends Ordered[LogEntry] {

  @volatile
  private[this] var _nextP = promise[LogEntry]()

  override def compare(other: LogEntry) = index compare other.index

  /** Appends `elem` to the list and returns its wrapper node. */
  def <<(term: Long,
         index: Long,
         cmd: String,
         args: Seq[String] = Array.empty[String],
         promise: Option[Promise[ReturnType]] = None) = {
    if (_nextP.isCompleted) truncate()
    val node = LogEntry(term, index, cmd, args, promise, this)
    _nextP.success(node)
    node
  }

  def nextP = _nextP

  def nextF = _nextP.future

  /** @return true iff this entry is the last entry */
  def isLast = !nextF.isCompleted

  /** Forwards `cmd` with `args` to the delegate.
    *
    * If a promise was attached to this log entry, fulfills that promise. Should be invoked at most once for each
    * log entry.
    *
    * @todo TODO handle exceptions from application
    * @return return value from the application
    */
  def apply(_delegate: Application): ReturnType = {
    val ret = _delegate(cmd, args)
    result.map(_.success(ret))
    result
  }

  /** @return previous log entry */
  protected[raft] def prev = _prev

  private[this] def truncate() {
    var ptr = this
    while (!ptr.isLast) ptr = Await.result(ptr.nextF, 0 nanos)
    ptr.nextP.failure(new TruncatedLogException)
    _nextP = promise[LogEntry]()
  }

  private[LogEntry] def prev_=(e: LogEntry) = _prev = e
}

object LogEntry {
  def apply(term: Long,
            index: Long,
            cmd: String,
            args: Seq[String] = Array.empty[String],
            result: Option[Promise[ReturnType]] = None,
            prev: LogEntry = sentinel) =
    new LogEntry(term, index, cmd, args, result, prev)

  /** @return sentinel entry that points to itself */
  def sentinel: LogEntry = {
    val s: LogEntry = new LogEntry(0, 0, "SENTINEL", Nil, None, null)
    s.prev = s
    s
  }
}