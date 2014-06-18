package com.jimjh.raft

import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar
import org.scalatest.{FlatSpec, Matchers}

/** Specs for the [[ElectionTimerComponent.ElectionTimer]].
  *
  * @author Jim Lim - jim@jimjh.com
  */
class ElectionTimerSpec
  extends FlatSpec
  with Matchers
  with Eventually
  with SpanSugar {

  class Delegate extends ElectionTimerDelegate {
    @volatile var triggered = 0

    def timeout() {
      triggered += 1
    }
  }

  trait Fixture extends ElectionTimerComponent {
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

  it should "allow restart even after an exception" in new ElectionTimerComponent {

    object delegate extends ElectionTimerDelegate {
      @volatile var triggered = 0

      def timeout() {
        triggered += 1
        throw new RuntimeException("w")
      }
    }

    val timer = new ElectionTimer(delegate)
    val maxWait = timer.timeoutMinMs + timer.timeoutRangeMs

    timer.restart()
    eventually(timeout(scaled(maxWait milliseconds))) {
      delegate.triggered should be(1)
    }

    timer.restart()
    eventually(timeout(scaled(maxWait milliseconds))) {
      delegate.triggered should be(2)
    }
  }

  it should "throw an IAE if _delegate is null" in new ElectionTimerComponent {
    intercept[IllegalArgumentException](new ElectionTimer(null))
  }
}
