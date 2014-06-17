package com.jimjh.raft

import java.util.concurrent.{ScheduledFuture, Executors}
import java.util.concurrent.TimeUnit._
import scala.Some
import scala.concurrent.{future, ExecutionContext}
import ExecutionContext.Implicits.global

trait HeartBeatDelegate {
  /** Invoked by the HeartBeat at timeout. */
  protected[raft] def pulse(term: Long): Unit
}

object HeartBeatDefaults {
  val Period = 150
}

/** Sends periodic heartbeats.
  *
  * @author Jim Lim - jim@jimjh.com
  */
class HeartBeat(private[this] val _delegate: HeartBeatDelegate,
                private[this] val _term: Long,
                val period: Int = HeartBeatDefaults.Period) {

  private[this] val _scheduler = Executors.newScheduledThreadPool(1)
  private[this] var _future = Option.empty[ScheduledFuture[_]]
  private[this] val _task = new Runnable {
    override def run(): Unit = {
      future {
        _delegate.pulse(_term)
      }
    }
  }

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
