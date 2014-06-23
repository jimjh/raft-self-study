package com.jimjh.raft

import java.util.concurrent.locks.ReentrantLock

import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise, promise}
import scala.language.{implicitConversions, postfixOps}

/** Cake Wrapper for Log.

  * @author Jim Lim - jim@jimjh.com
  */
trait LogComponent {

  val log: Log

  implicit def toSugaredLock(lock: ReentrantLock) = new SugaredLock(lock)

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
    * fast disk I/O). We sacrifice random access, but that is rarely used, according to the RAFT paper. A sentinel is
    * used to mark the start of the list with index 0. Actual commands start at index 1.
    *
    * == Intended Usage ==
    * {{{
    *   val log = new Log(delegate).start()
    *   log.append(w)
    * }}}
    *
    * @param _delegate target application to which commands are forwarded
    */
  class Log(_delegate: Application) {

    notNull(_delegate, "_delegate")

    private[this] val _logger = Logger(LoggerFactory getLogger "Log")

    /** Entry marking the start of the log. */
    private[this] val _sentinel = new LogEntry(0, 0, "SENTINEL")

    /** Serializable sequence of log entries. */
    private[this] val _logs = new SugaredList(_sentinel)

    /** Write lock for [[_commitIndex]]. */
    private[this] val _commitLock = new ReentrantLock()
    private[this] val _hasCommits = _commitLock.newCondition()

    /** Pointer to the last log entry. */
    @volatile
    private[this] var _last: LogIndex = _logs

    /** Index of the last log entry that was applied. */
    @volatile
    private[this] var _lastApplied: LogIndex = _logs

    @volatile
    private[this] var _commitIndex = 0L

    protected[raft] def last: LogIndex = _last

    protected[raft] def lastEntry: LogEntry = _last.elem

    protected[raft] def lastIndexNum: Long = lastEntry.index

    protected[raft] def lastApplied = _lastApplied.index

    protected[raft] def commitIndex = _commitIndex

    /** Updates commit index to `index`.
      *
      * @param index commit index
      * @throws IllegalStateException if `index` < [[_commitIndex]], or if `index` > [[lastIndexNum]]
      */
    protected[raft] def commitIndex_=(index: Long) {
      _commitLock {
        require(index >= _commitIndex, s"Cannot decrease commitIndex from ${_commitIndex} to $index.")
        require(index <= lastIndexNum, s"Cannot set commitIndex greater than the lastIndex=$lastIndexNum.")
        _commitIndex = index
        _hasCommits.signalAll()
      }
    }

    /** Appends a single log entry with the given `term`, `command`, and `arguments` to the log.
      *
      * @param term election term
      * @param cmd  command
      * @param args arguments
      * @param promise promised result (from application)
      * @return log entry - the log entry that was created and appended
      */
    protected[raft] def append(term: Long,
                               cmd: String,
                               args: Seq[String] = Array.empty[String],
                               promise: Option[Promise[ReturnType]] = None,
                               index: LogIndex = _last): LogEntry = {
      _logs.synchronized {
        _last = index << LogEntry(term, lastIndexNum + 1, cmd, args, promise)
        _last.elem
      }
    }

    /** Launches a new background task that attempts to apply all log entries up till [[_commitIndex]].
      *
      * @return this
      */
    protected[raft] def start() = {
      new Thread(new Runnable {
        // [IMPORTANT] this should be the only thread that has write access to _lastApplied
        override def run() = {
          Thread.currentThread().setName("LogApplicator")
          _logger.debug("LogApplicator started.")
          keepApplying()
        }
      }).start()
      this
    }

    /** Keeps applying logs until [[_commitIndex]], then waits. */
    private[this] def keepApplying() =
      while (!Thread.currentThread().isInterrupted) {

        var targetIndex = lastApplied
        _commitLock {
          // take lock, read value, make decision
          if (lastApplied < _commitIndex) {
            targetIndex = _commitIndex
          } else _hasCommits.await()
        }

        // release lock while applying committed log entries
        while (lastApplied < targetIndex) {
          if (_lastApplied.isLast) // make sure .next is valid
            throw new IndexOutOfBoundsException(s"commitIndex=$targetIndex exceeded the end of the _logs sequence.")
          val next = Await.result(_lastApplied.nextF, 0 nanos)
          next.elem(_delegate) // forward log entry's command to delegate
          _lastApplied = next
        }
      }
  }

  /** Skeletal implementation of a linked list.
    *
    * @todo TODO persistent state using append, flush
    * @todo TODO initialize from persisted state
    * @param elem log entry
    */
  private[raft] class SugaredList(val elem: LogEntry,
                                  _prev: LogIndex = null)
    extends LogIndex {

    private[this] val _logger = Logger(LoggerFactory getLogger "SugaredList")

    private[this] var _nextP = promise[SugaredList]()

    @volatile
    private[this] var _nextF: Future[SugaredList] = _nextP.future

    private def nextP = _nextP

    /** Appends `elem` to the list and returns its wrapper node. */
    override protected[raft] def <<(entry: LogEntry) = {
      if (nextP.isCompleted) truncate()
      val node = new SugaredList(entry, this)
      nextP.success(node)
      node
    }

    override protected[raft] def nextF = _nextF

    override protected[raft] def prev = _prev

    override protected[raft] def isLast = !nextF.isCompleted

    override protected[raft] def term = elem.term

    override protected[raft] def index = elem.index

    private[this] def truncate() {
      _logger.warn(s"Truncating log at index $index")
      var ptr = this
      while (!ptr.isLast) ptr = Await.result(ptr.nextF, 0 nanos)
      ptr.nextP.failure(truncated)
      _nextP = promise[SugaredList]()
      _nextF = _nextP.future
    }
  }

  /** Pimps [[java.util.concurrent.locks.ReentrantLock]] to provide a nicer syntax for locking and unlocking. */
  private[LogComponent] class SugaredLock(lock: ReentrantLock) {
    def apply[T](block: => T): T = {
      lock.lock()
      try block
      finally lock.unlock()
    }
  }

  private[this] def truncated = new TruncatedLogException

  class TruncatedLogException extends Exception

}

/** An entry in the [[LogComponent# L o g]].
  *
  * Log entries are immutable.
  */
case class LogEntry(term: Long,
                    index: Long,
                    cmd: String,
                    args: Seq[String] = Array.empty[String],
                    promise: Option[Promise[ReturnType]] = None)
  extends Ordered[LogEntry] {

  /** Forwards `cmd` with `args` to the delegate.
    *
    * If a promise was attached to this log entry, fulfills that promise. Should be invoked at most once for each
    * log entry.
    *
    * @todo TODO handle exceptions from application
    * @return return value from the application
    */
  protected[raft] def apply(_delegate: Application): ReturnType = {
    val result = _delegate(cmd, args)
    promise.map(_.success(result))
    result
  }

  override def compare(that: LogEntry) = index compare that.index
}

trait LogIndex
  extends Ordered[LogIndex] {

  // TODO resolve dilemma - restrict api, or allow subclasses?

  protected[raft] def nextF: Future[LogIndex]

  protected[raft] def prev: LogIndex

  protected[raft] def elem: LogEntry

  protected[raft] def index: Long

  protected[raft] def term: Long

  protected[raft] def isLast: Boolean

  protected[raft] def <<(entry: LogEntry): LogIndex

  override def compare(that: LogIndex): Int = index compare that.index
}
