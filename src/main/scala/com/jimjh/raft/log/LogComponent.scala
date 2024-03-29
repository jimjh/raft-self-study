package com.jimjh.raft.log

import java.io.Closeable
import java.util.concurrent.locks.ReentrantLock

import com.jimjh.raft._
import com.jimjh.raft.annotation.threadsafe
import com.typesafe.scalalogging.slf4j.Logger

import scala.concurrent.duration._
import scala.concurrent.{Await, Promise}
import scala.language.{implicitConversions, postfixOps}

/** Cake Wrapper for Log.

  * @author Jim Lim - jim@jimjh.com
  */
trait LogComponent {

  val log: Log // #injectable

  implicit val persistence: PersistenceComponent#Persistence // #injected

  /** slf4j logger. */
  protected[raft] val logger: Logger // #injected

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
    * @todo TODO initialize from persisted state
    *
    * @param _delegate target application to which commands are forwarded
    */
  @threadsafe
  class Log(_delegate: Application) extends Closeable {

    notNull(_delegate, "_delegate")

    /** Serializable sequence of log entries. */
    private[this] val _logs = LogEntry.sentinel

    /** Write lock for [[_commit]]. */
    private[this] val _commitLock = new ReentrantLock()

    private[this] val _hasCommits = _commitLock.newCondition()

    /** Pointer to the last log entry. */
    @volatile
    private[this] var _last: LogEntry = _logs

    /** Index of the last log entry that was applied. */
    @volatile
    private[this] var _lastApplied: LogEntry = _logs

    /** Index of the committed log entry. */
    @volatile
    private[this] var _commit = 0L

    private[this] val _applicator = new Thread(new Runnable {
      // [IMPORTANT] this should be the only thread that has write access to _lastApplied
      override def run() = {
        Thread.currentThread().setName("raft.LogApplicator")
        logger.trace("LogApplicator started.")
        keepApplying()
      }
    })

    def last: LogEntry = _last

    def lastIndex: Long = last.index

    def lastApplied = _lastApplied.index

    def commit = _commit

    /** Updates commit index to `index`. Thread-safe.
      *
      * @param index new commit index
      */
    def commit_=(index: Long) {
      _commitLock {
        if (index > commit) {
          logger.debug(s"Advancing commit from $commit to $index.")
          _commit = index
          _hasCommits.signalAll()
        }
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
    def append(term: Long,
               cmd: String,
               args: Seq[String] = Array.empty[String],
               promise: Option[Promise[ReturnType]] = None,
               from: LogEntry = _last): LogEntry = _logs.synchronized {
      logger.trace(s"Appending new entry after index ${from.index} with command $cmd.")
      _last = from <<(term, cmd, args, promise)
      _last
    }

    def appendEntries(term: Long,
                      entries: Seq[com.jimjh.raft.rpc.Entry],
                      from: LogEntry = _last) = _logs.synchronized {
      logger.trace(s"Appending entries after index ${from.index}.")
      var prev = from
      for (entry <- entries) {
        _last = prev <<(term, entry.cmd, entry.args)
        prev = _last
      }
      _last
    }

    /** @return last log entry with a match index and term */
    def findLast(index: Long, term: Long): Option[LogEntry] = {
      var entry = last
      while (entry.index > 0 && entry.index > index) entry = entry.prev
      entry.index == index && entry.term == term match {
        case true => Some(entry)
        case false => None
      }
    }

    /** Launches a new background task that attempts to apply all log entries up till [[commit]].
      *
      * @return this
      */
    def start() = {
      rebuild()
      _applicator.start()
      this
    }

    /** Stops the background task.
      *
      * @return this
      */
    override def close() = {
      _applicator.interrupt()
    }

    /** Rebuilds log from disk contents. */
    private[this] def rebuild() = {
      // TODO
    }

    /** Keeps applying logs until [[commit]], then waits. */
    private[this] def keepApplying() =
      while (!Thread.currentThread().isInterrupted) {

        var targetIndex = lastApplied
        _commitLock {
          // take lock, read value, make decision
          if (lastApplied < _commit) {
            targetIndex = _commit
          } else _hasCommits.await()
        }

        // release lock while applying committed log entries
        while (lastApplied < targetIndex) {
          val next = Await.result(_lastApplied.nextF, Duration.Inf)
          next(_delegate) // forward log entry's command to delegate
          _lastApplied = next
        }
      }
  }

  /** Pimps [[java.util.concurrent.locks.ReentrantLock]] to provide a nicer syntax for locking and unlocking. */
  private[LogComponent] class SugaredLock(lock: ReentrantLock) {
    def apply[T](block: => T): T = {
      lock.lock()
      try block
      catch {
        case e: InterruptedException =>
          logger.warn("Releasing lock due to thread interruption", e)
          throw e
      }
      finally lock.unlock()
    }
  }

}
