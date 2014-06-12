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
  class Log(delegate: Application) {

    /** Forwards `cmd` with `args` to the contained application. */
    protected[raft] def apply(cmd: String, args: Array[String]) =
      delegate.apply(cmd, args)
  }

}
