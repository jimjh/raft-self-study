package com.jimjh.raft

import java.util.concurrent.TimeUnit.MILLISECONDS
import java.util.concurrent.{Executors, ScheduledFuture}

import com.typesafe.scalalogging.slf4j.Logger
import org.slf4j.LoggerFactory

import scala.util.Random

/** Timer delegate that receives invocations from the timer.
  *
  * @author Jim Lim - jim@jimjh.com
  */
trait Timeoutable {
  /** Invoked by the ElectionTimer at timeout. */
  def timeout(): Unit
}

trait ElectionTimerComponent {

  object ElectionTimer {
    val TimeoutRangeMs = 2500
    val TimeoutMinMs = 350
  }

  /** Resettable timer that controls election timeouts. Thread-safe.
    *
    * Each timeout is randomly chosen from an interval. Provide a delegate object to receive
    * [[Timeoutable.timeout]] calls when the timeout is triggered.
    *
    * {{{
    *   TIMEOUT_MIN_MS <= timeout <= TIMEOUT_MIN_MS + TIMEOUT_RANGE_MS
    * }}}
    *
    * @author Jim Lim - jim@jimjh.com
    */
  class ElectionTimer(_delegate: Timeoutable,
                      val timeoutRangeMs: Int = ElectionTimer.TimeoutRangeMs,
                      val timeoutMinMs: Int = ElectionTimer.TimeoutRangeMs) {

    notNull(_delegate, "_delegate")

    private[this] val _random = new Random()
    private[this] val _scheduler = Executors.newScheduledThreadPool(1)
    private[this] var _future = Option.empty[ScheduledFuture[_]]
    private[this] val _logger = Logger(LoggerFactory getLogger "ElectionTimer")
    private[this] val _task = new Runnable {
      override def run() = _delegate.timeout()
    }

    // allow me a little bit of debugging convenience
    _scheduler.submit(new Runnable {
      override def run() {
        Thread.currentThread().setName("raft.ElectionTimer")
      }
    })

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

}
