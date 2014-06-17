package com.jimjh.raft

import org.scalatest.{FlatSpec, Matchers}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar

/** Specs for the [[ElectionTimer]].
  *
  * @author Jim Lim - jim@jimjh.com
  */
class ElectionTimerSpec
  extends FlatSpec
  with Matchers
  with Eventually
  with SpanSugar {

  class Delegate extends ElectionTimerDelegate {
    var triggered = 0

    def timeout() {
      triggered += 1
    }
  }

  trait Fixture {
    val delegate = new Delegate
    val timer = new ElectionTimer(delegate)
  }

  it should "cancel" in new Fixture {
    val double = 2 * (timer.timeoutMinMs + timer.timeoutRangeMs)
    timer.restart().cancel()

    Thread.sleep(double)
    delegate.triggered should be(0)
  }

  it should "restart" in new Fixture {
    val pentuple = 5 * (timer.timeoutMinMs + timer.timeoutRangeMs)
    timer.restart().restart().restart().restart()

    Thread.sleep(pentuple)
    delegate.triggered should be(1)
  }

  it should "trigger timeout on the given delegate" in new Fixture {
    val maxWait = timer.timeoutMinMs + timer.timeoutRangeMs
    timer.restart()

    eventually(timeout(scaled(maxWait milliseconds))) {
      delegate.triggered should be(1)
    }
  }
}
