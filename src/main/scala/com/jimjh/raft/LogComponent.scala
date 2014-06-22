package com.jimjh.raft

import com.twitter.util.Promise

import scala.language.implicitConversions

/** Cake Wrapper for Log.

  * @author Jim Lim - jim@jimjh.com
  */
trait LogComponent {

  val log: Log

  /** Manages an ordered list of commands that will be applied on the delegate app. Thread-safe.
    *
    * The log is responsible for
    *
    * - file I/O,
    * - compaction (snapshotting),
    * - application,
    * - flushing (persistence),
    * - recovery (persistence).
    *
    * I decided to use a DoubleLinkedList for now, because linked lists are easy to persist using append and flush (for
    * fast disk I/O). We sacrifice random access, but that is rarely used, according to the RAFT paper.
    *
    * @todo TODO the locks/volatile here are shit
    */
  private[raft] class Log(_delegate: Application) {

    notNull(_delegate, "_delegate")

    private[this] val _sentinel = new LogEntry(0, 0, "SENTINEL")
    private[this] val _logs = new SugaredList(_sentinel)

    @volatile
    private[this] var _last = _logs

    protected[raft] def lastEntry = _last.elem

    protected[raft] def lastIndex = lastEntry.index

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

    /** Helper method for [[Log.commit]] */
    private[this] def applyUntil(index: Long) = {
      var result: ReturnType = None
      while (lastApplied < index) {
        if (_lastApplied.isLast) throw new IllegalStateException("#commit exceeded the end of the _logs list.")
        val next = _lastApplied.next
        result = next.elem(_delegate) // apply log entry on delegate
        _lastApplied = next
      }
      result
    }
  }

  /** An entry in the [[Log]]. */
  class LogEntry(val term: Long,
                 val index: Long,
                 val cmd: String,
                 val args: Array[String] = Array.empty[String],
                 val promise: Option[Promise[ReturnType]] = None)
    extends Ordered[LogEntry] {

    // TODO handle exceptions from application
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

    override def compare(that: LogEntry) = index compare that.index
  }

  // TODO persistent state using append, flush
  class SugaredList(val elem: LogEntry) {

    var next = this
    var prev: SugaredList = null

    /** Appends `elem` to the list and returns its wrapper node. */
    def !!(entry: LogEntry) = {
      next = new SugaredList(entry)
      next.prev = this
      next
    }

    def isLast = next eq this

    def term = elem.term

    def index = elem.index
  }

}