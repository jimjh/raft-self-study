package com.jimjh.raft

import org.scalatest.{Matchers, FlatSpec}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar
import scala.language.reflectiveCalls

/** Specs for the [[HeartBeat]].
  *
  * @author Jim Lim - jim@jimjh.com
  */
class HeartBeatSpec
  extends FlatSpec
  with Matchers
  with Eventually
  with SpanSugar {

  val AnyTerm = 31

  trait Fixture {
    val delegate = new HeartBeatDelegate {
      @volatile var triggered = 0

      override protected[raft] def pulse(term: Long) {
        triggered += 1
      }
    }
    val heartbeat = new HeartBeat(delegate, AnyTerm)
  }

  it should "start" in new Fixture {
    heartbeat.start()
    val pause = 11 * heartbeat.period
    eventually(timeout(scaled(pause milliseconds))) {
      delegate.triggered should be >= 10
    }
  }

  it should "cancel" in new Fixture {
    heartbeat.start().cancel()
    val twice = 2 * heartbeat.period
    Thread.sleep(twice)
    delegate.triggered should be(1) // triggered once at start
  }

  it should "trigger regularly even if the delegate is slow" in {
    val delegate = new HeartBeatDelegate {
      @volatile var triggered = 0

      override protected[raft] def pulse(term: Long) {
        triggered += 1
        Thread.sleep(20000)
      }
    }
    val heartbeat = new HeartBeat(delegate, AnyTerm).start()
    val pause = 30 * heartbeat.period
    eventually(timeout(scaled(pause milliseconds))) {
      delegate.triggered should be >= 10
    }
  }

  it should "throw an IAE if _delegate were null" in {
    intercept[IllegalArgumentException] {
      new HeartBeat(null, 0)
    }
  }
}
