package com.jimjh.raft

import java.util.concurrent.{ScheduledFuture, Executors, ScheduledExecutorService}
import java.util.concurrent.TimeUnit._
import scala.Some
import scala.concurrent.{future, ExecutionContext}
import ExecutionContext.Implicits.global

trait HeartBeatDelegate {
  /** Invoked by the HeartBeat at timeout. */
  protected[raft] def pulse(term: Long): Unit
}

/** Sends periodic heartbeats.
  *
  * @author Jim Lim - jim@jimjh.com
  */
class HeartBeat(private[this] val _delegate: HeartBeatDelegate,
                private[this] val _term: Long) {

  val TIMEOUT_PERIOD = 150

  private[this] val _scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(1)
  private[this] var _future: Option[ScheduledFuture[_]] = None
  private[this] val _task: Runnable = new Runnable {
    override def run(): Unit = {
      future {
        _delegate.pulse(_term)
      }
    }
  }

  /** Schedules periodic heartbeats. */
  def start(): HeartBeat = {
    this.synchronized {
      _future = Some(_scheduler.scheduleAtFixedRate(_task, 0, TIMEOUT_PERIOD, MILLISECONDS))
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
