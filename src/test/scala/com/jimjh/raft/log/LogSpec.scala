package com.jimjh.raft.log

import com.jimjh.raft._
import com.jimjh.raft.spec.UnitSpec

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, promise}

/** Specs for [[LogComponent.Log]]
  *
  * @author Jim Lim - jim@jimjh.com
  */
class LogSpec extends UnitSpec {


  trait Fixture extends LogComponent {

    val AnyTerm = 5
    val AnyCommand = "any.command"

    // dummy application that just prepends commands
    object delegate extends Application {
      var history = List[String]()

      override def apply(cmd: String, args: Seq[String]): Option[Any] = {
        history = cmd +: history
        None
      }
    }

    // test subject
    val log = new Log(delegate).start()
  }

  it should "append a new LogEntry" in new Fixture {
    log.append(AnyTerm, AnyCommand, Array("arg1"))

    log.lastIndex should be(1)
    log.last.cmd should be(AnyCommand)
    log.last.args should be(Array("arg1"))
    log.lastApplied should be(0)
  }

  it should "append 100 new log entries" in new Fixture {
    (1 until 100).foreach(log.append(_, AnyCommand))

    log.lastIndex should be(99)
    log.last.cmd should be(AnyCommand)
    log.lastApplied should be(0)
  }

  trait PopulatedFixture extends Fixture {
    val PopulateCount = 100
    val Term = 1
    (1 until 100 + 1).foreach(i => log.append(Term, s"command#$i"))
  }

  it should "apply (asynchronously) commands up till the commit index" in new PopulatedFixture {
    val CommitIndex = 93L
    log.commitIndex = CommitIndex

    eventually(timeout(scaled(1000 milliseconds))) {
      log.lastApplied should be(CommitIndex)
      delegate.history.length should be(CommitIndex)
      delegate.history.head should be(s"command#$CommitIndex")
    }
  }

  it should "throw up an IAE if commitIndex > lastIndexNum" in new PopulatedFixture {
    intercept[IllegalArgumentException](log.commitIndex = PopulateCount + 1)
  }

  it should "throw up an IAE if new commitIndex < existing commitIndex" in new PopulatedFixture {
    log.commitIndex = PopulateCount
    intercept[IllegalArgumentException](log.commitIndex = PopulateCount - 1)
  }

  it should "allow concurrent committing and appending" in new Fixture {
    val term = 10
    val count = 100
    val conductor = new Conductor

    import conductor._

    thread("appender") {
      (0 until count) foreach { i => log.append(term, s"command#$i")}
      waitForBeat(1) // wait for committer to start
      (count until 2 * count) foreach { i => log.append(term, s"command#$i")}
    }

    thread("committer") {
      waitForBeat(1)
      log.commitIndex = count // commit all
      waitForBeat(2)
      log.commitIndex = 2 * count // commit all
    }

    whenFinished {
      eventually {
        log.lastApplied should be(2 * count)
        delegate.history.length should be(2 * count)
      }
    }
  }

  it should "fulfill promises, if any" in new Fixture {

    val conductor = new Conductor

    import conductor._

    val p = promise[ReturnType]()
    val entry = log.append(AnyTerm, AnyCommand, Array("arg1"), Some(p))

    thread("reader") {
      Await.result(p.future, Duration.Inf) should be(None)
    }

    thread("writer") {
      entry(delegate)
    }
  }

  it should "update commit index" in new PopulatedFixture {
    log.commitIndex = 10L
    log.commitIndex should be(10L)
  }

  it should "advance a given log index w. a future" in new PopulatedFixture {

    import scala.concurrent.ExecutionContext.Implicits.global

    val last = log.last
    var fulfilled = false
    last.nextF.map { _ => fulfilled = true}

    fulfilled should be(false)
    log.append(0, "any.cmd")
    eventually {
      fulfilled should be(true)
    }
  }

  it should "truncate w. exceptions" in new PopulatedFixture {

    import scala.concurrent.ExecutionContext.Implicits.global

    // attach a hook to the last entry's next ptr
    val last = log.last

    @volatile
    var fulfilled = 0

    last.nextF
      .map { _ => fulfilled = 1}
      .onFailure { case _ => fulfilled = 2}

    // truncate
    val prev = last.prev
    log.append(0, "any.cmd", Array.empty[String], None, prev)
    eventually {
      fulfilled should be(2)
    }
  }
}
