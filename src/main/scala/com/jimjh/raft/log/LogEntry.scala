package com.jimjh.raft.log

import java.io.ObjectStreamException

import com.jimjh.raft._
import com.jimjh.raft.annotation.threadsafe

import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps

/** Skeletal implementation of a linked list.
  *
  * Only nextF and nextP may be mutable. All other fields, including prev, are immutable. The start of the list is
  * called a *sentinel*, which is a special entry that points to itself.
  *
  * @param term term during which this entry was created
  * @param index log index
  * @param result optional promise that will be fulfilled after `cmd` is applied
  * @param _prev previous log entry
  */
@threadsafe
class LogEntry(val term: Long,
               val index: Long,
               val cmd: String,
               val args: Seq[String] = Array.empty[String],
               @transient val result: Option[Promise[ReturnType]] = None,
               @transient private[raft] var _prev: LogEntry = LogEntry.sentinel)
  extends Ordered[LogEntry]
  with Serializable {

  @transient
  @volatile
  private[this] var _nextP = promise[LogEntry]()

  private[this] type Persistence = PersistenceComponent#Persistence

  override def compare(other: LogEntry) = index compare other.index

  /** Appends `elem` to the list and returns its wrapper node. */
  def <<(term: Long,
         cmd: String,
         args: Seq[String] = Array.empty[String],
         promise: Option[Promise[ReturnType]] = None)
        (implicit persistence: Persistence) = {
    if (_nextP.isCompleted) truncate()
    val node = LogEntry(term, index + 1, cmd, args, promise, this)

    persistence appendLog node
    _nextP success node
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

  @throws(classOf[ObjectStreamException])
  def readResolve: Object = {
    // re-initialize transient members
    LogEntry(term, index, cmd, args)
  }

  override def toString = (term, index, cmd, args).toString()

  /** @return previous log entry */
  protected[raft] def prev = _prev

  private[this] def truncate()(implicit persistence: Persistence) {
    persistence truncateLog index
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