package com.jimjh.raft

import java.util.concurrent.{ScheduledFuture, Executors}
import java.util.concurrent.TimeUnit.MILLISECONDS
import scala.util.Random

/** Timer delegate that receives invocations from the timer.
  *
  * @author Jim Lim - jim@jimjh.com
  */
trait ElectionTimerDelegate {
  /** Invoked by the ElectionTimer at timeout. */
  protected[raft] def timeout(): Unit
}

object ElectionTimerDefaults {
  val TIMEOUT_RANGE_MS = 2000
  val TIMEOUT_MIN_MS = 450
}

/** Resettable timer that controls election timeouts. Thread-safe.
  *
  * Each timeout is randomly chosen from an interval. Provide a delegate object to receive
  * [[ElectionTimerDelegate.timeout( )]] calls when the timeout is triggered.
  *
  * {{{
  *   TIMEOUT_MIN_MS ≤ timeout ≤ TIMEOUT_MIN_MS + TIMEOUT_RANGE_MS
  * }}}
  *
  * @author Jim Lim - jim@jimjh.com
  */
class ElectionTimer(private[this] val _delegate: ElectionTimerDelegate,
                    val timeoutRangeMs: Int = ElectionTimerDefaults.TIMEOUT_RANGE_MS,
                    val timeoutMinMs: Int = ElectionTimerDefaults.TIMEOUT_RANGE_MS) {

  private[this] val _random = new Random()
  private[this] val _scheduler = Executors.newScheduledThreadPool(1)
  private[this] var _future = Option.empty[ScheduledFuture[_]]
  private[this] val _task = new Runnable {
    override def run(): Unit = {
      _delegate.timeout()
    }
  }

  /** Cancels any existing tasks and restarts the timer */
  def restart(): ElectionTimer = {
    this.synchronized {
      cancel()
      _future = Some(_scheduler.schedule(_task, timeout, MILLISECONDS))
    }
    this
  }

  /** Cancels any existing tasks */
  def cancel(): ElectionTimer = {
    this.synchronized {
      _future map (_.cancel(false))
    }
    this
  }

  private[this] def timeout = _random.nextInt(timeoutRangeMs) + timeoutMinMs
}
