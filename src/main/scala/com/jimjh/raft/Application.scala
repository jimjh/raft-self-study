package com.jimjh.raft

/** Interface for the application contained and controlled by RAFT.
  *
  * Applications implement this interface to receive committed commands from the log.
  *
  * On success, the `lastApplied` counter is incremented. On exception, the behavior is undefined.
  *
  * As documented in the RAFT paper, it's up to the application to prevent duplicate
  * executions of the same command _e.g._ assign unique serial numbers to each
  * command and ignore commands that have been executed.
  *
  * @author Jim Lim - jim@jimjh.com
  */
trait Application {

  /** Applies `cmd` with `args` to the application.
    *
    * @todo TODO exceptions should be forwarded to the client
    * @param cmd   command
    * @param args  array of command arguments
    * @throws RuntimeException on error
    */
  def apply(cmd: String, args: Seq[String]): ReturnType
}