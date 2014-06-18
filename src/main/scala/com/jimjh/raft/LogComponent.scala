package com.jimjh.raft

/** Cake Wrapper for Log.
  *
  * @author Jim Lim - jim@jimjh.com
  */
trait LogComponent {

  val log: Log

  /** Manages an ordered list of commands that will be applied on the delegate app.
    *
    * The log is responsible for
    *
    * - file I/O,
    * - compaction (snapshotting),
    * - application,
    * - flushing,
    * - recovery etc.
    */
  class Log(private[this] val _delegate: Application) {

    // TODO persistent state
    private[this] var _logs: Seq[LogEntry] = new LogEntry(0, 0, "", Array.empty[String]) :: Nil

    /** Forwards `cmd` with `args` to the contained application. */
    protected[raft] def apply(cmd: String, args: Array[String]) =
      _delegate.apply(cmd, args)

    protected[raft] def last = _logs.last
  }

  /** An entry in the [[Log]]. */
  class LogEntry(val term: Long,
                 val index: Long,
                 val cmd: String,
                 val args: Array[String])

}
