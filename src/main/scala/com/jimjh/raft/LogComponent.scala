package com.jimjh.raft

import scala.collection.mutable
import com.twitter.util.Promise
import scala.language.implicitConversions

/** Cake Wrapper for Log.

  * @author Jim Lim - jim@jimjh.com
  */
trait LogComponent {

  val log: Log

  private val emptyNode = new mutable.LinkedList[LogEntry]()

  implicit def linkedListToSugaredList(list: mutable.LinkedList[LogEntry]) = new SugaredList(list)

  /** Manages an ordered list of commands that will be applied on the delegate app. Thread-safe.
    *
    * The log is responsible for
    *
    * - file I/O,
    * - compaction (snapshotting),
    * - application,
    * - flushing,
    * - recovery etc.
    */
  // TODO persistent state
  private[raft] class Log(private[this] val _delegate: Application) {

    notNull(_delegate, "_delegate")

    private[this] val _sentinel = new LogEntry(0, 0, "SENTINEL")
    private[this] val _logs: mutable.LinkedList[LogEntry] = _sentinel !! emptyNode

    @volatile
    private[this] var _last = _logs

    protected[raft] def last = _last.elem

    protected[raft] def lastIndex = last.index

    /** Write lock - synchronizes access to the `_last` variable and its `next` pointer */
    private[this] val appendLock = new Object()

    /** Appends a single log entry with the given term, command, and arguments to the log.
      *
      * This acquires the write lock.
      *
      * @param term election term
      * @param cmd  command
      * @param args arguments
      */
    protected[raft] def append(term: Long,
                               cmd: String,
                               args: Array[String] = Array.empty[String],
                               promise: Option[Promise[ReturnType]] = None): LogEntry =
      appendLock.synchronized {
        val entry = new LogEntry(term, lastIndex + 1, cmd, args, promise)
        _last = _last !! entry
        entry
      }

    @volatile
    private[this] var _lastApplied = _logs

    protected[raft] def lastApplied = _lastApplied.elem.index

    /** Read lock - synchronizes access to `apply` and the `_lastApplied` variable. */
    private[this] val commitLock = new Object()

    /** Applies commands in the log from `lastApplied` until `till`.
      *
      * @param till index of final log entry to apply
      * @return None if no log entries were applied; results of the last application, otherwise
      */
    protected[raft] def commit(till: Long) =
      commitLock.synchronized {
        require(till <= lastIndex, s"`till` must be at most $lastIndex")
        till compare lastApplied match {
          case -1 | 0 => None
          case 1 => applyUntil(till)
        }
      }

    private[this] def applyUntil(index: Long) = {
      while (lastApplied < index) {
        val next = _lastApplied.next
        if (next == emptyNode) throw new IllegalStateException("#commit exceeded the end of the _logs list.")
        next.elem(_delegate) // apply log entry on delegate
        _lastApplied = next
      }
    }

    // TODO handle exceptions from application
  }

  /** An entry in the [[Log]]. */
  class LogEntry(val term: Long,
                 val index: Long,
                 val cmd: String,
                 val args: Array[String] = Array.empty[String],
                 val promise: Option[Promise[ReturnType]] = None) {
    def !!(next: mutable.LinkedList[LogEntry]) =
      new mutable.LinkedList[LogEntry](this, next)

    /** Forwards `cmd` with `args` to the delegate.
      *
      * If a promise was attached to this log entry, fulfill the promise.
      * @return return value from the application
      */
    def apply(_delegate: Application) = {
      val result = _delegate.apply(cmd, args)
      promise.map(_.setValue(result))
      result
    }
  }

  /** Adds a special `!!` operator for append, using the Pimp My Library pattern. */
  class SugaredList(list: mutable.LinkedList[LogEntry]) {
    def !!(elem: LogEntry) = {
      list.next = elem !! emptyNode
      list.next
    }
  }

}