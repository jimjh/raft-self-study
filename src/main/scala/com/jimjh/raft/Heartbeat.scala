package com.jimjh.raft

import java.util.concurrent.TimeUnit.{MILLISECONDS, SECONDS}
import java.util.concurrent._

import scala.concurrent.{ExecutionContext, future}

trait HeartBeatDelegate {
  /** Invoked by the HeartBeat at timeout. */
  protected[raft] def pulse(term: Long): Unit
}

object HeartBeatDefaults {
  val Period = 150
}

/** Sends periodic heartbeats.
  *
  * A single thread is used to schedule periodic heartbeats. Each heartbeat invokes [[HeartBeatDelegate.pulse]] using a
  * thread from a cached thread pool. Every pulse must terminate within 2 heartbeats in order to guarantee regular
  * heartbeats. If pulses block, new threads need to be created, which may cause performance to degrade over time.
  *
  * @author Jim Lim - jim@jimjh.com
  */
class HeartBeat(private[this] val _delegate: HeartBeatDelegate,
                private[this] val _term: Long,
                val period: Int = HeartBeatDefaults.Period) {

  private[this] val _scheduler = Executors.newScheduledThreadPool(1)
  private[this] var _future = Option.empty[ScheduledFuture[_]]
  private[this] val _task = new Runnable {
    override def run() {
      future {
        _delegate.pulse(_term)
      }
    }
  }

  /** ExecutionContext for scala futures */
  private[this] implicit lazy val _context = new ExecutionContext {

    // start with 2 threads, assuming that each pulse completes within 2 periods
    val coreThreadPoolSize = 2

    // discard idle threads after 10 heartbeats
    val cacheTimeout = 10 * period

    // create a cached thread pool with a non-zero number of core threads
    val threadPool =
      new ThreadPoolExecutor(
        coreThreadPoolSize,
        Integer.MAX_VALUE,
        cacheTimeout, SECONDS,
        new SynchronousQueue[Runnable])

    val reporter = ExecutionContext.defaultReporter

    def execute(runnable: Runnable) = threadPool.submit(runnable)

    def reportFailure(t: Throwable) = reporter(t)
  }

  // allow me a little bit of debugging convenience
  _scheduler.submit(new Runnable {
    override def run() {
      Thread.currentThread().setName("raft.HeartBeat")
    }
  })

  /** Schedules periodic heartbeats. */
  def start(): HeartBeat = {
    this.synchronized {
      _future = Some(_scheduler.scheduleAtFixedRate(_task, 0, period, MILLISECONDS))
    }
    this
  }

  /** Cancels heartbeats. */
  def cancel(): HeartBeat = {
    this.synchronized {
      _future map (_.cancel(false))
    }
    this
  }

}
