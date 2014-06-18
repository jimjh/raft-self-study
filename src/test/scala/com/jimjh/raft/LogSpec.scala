package com.jimjh.raft

import org.scalatest.{Matchers, FlatSpec}
import org.scalatest.concurrent.Conductors
import com.twitter.util.{Await, Promise}

/** Specs for [[LogComponent.Log]]
  *
  * @author Jim Lim - jim@jimjh.com
  */
class LogSpec
  extends FlatSpec
  with Matchers
  with Conductors {


  trait Fixture extends LogComponent {

    val AnyTerm = 5
    val AnyCommand = "any.command"

    // dummy application that just prepends commands
    object delegate extends Application {
      var history = List[String]()

      override def apply(cmd: String, args: Array[String]): Option[Any] = {
        history = cmd +: history
        None
      }
    }

    // test subject
    val log = new Log(delegate)
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

  it should "apply (synchronously) commands up till the commit index" in new PopulatedFixture {
    val CommitIndex = 93
    log.commit(CommitIndex)

    log.lastApplied should be(CommitIndex)
    delegate.history.length should be(CommitIndex)
    delegate.history.head should be(s"command#$CommitIndex")
  }

  it should "throw up an IAE if commit index is out of bounds" in new PopulatedFixture {
    intercept[IllegalArgumentException](log.commit(PopulateCount + 1))
  }

  it should "do nothing if the entries have already been applied" in new PopulatedFixture {
    val CommitIndex = 50
    log.commit(CommitIndex)
    log.commit(CommitIndex)

    log.lastApplied should be(CommitIndex)
    delegate.history.length should be(CommitIndex)
    delegate.history.head should be(s"command#$CommitIndex")
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
      log.commit(count) // commit all
      waitForBeat(2)
      log.commit(2 * count) // commit all
    }

    whenFinished {
      log.lastApplied should be(2 * count)
      delegate.history.length should be(2 * count)
    }
  }

  it should "fulfill promises, if any" in new Fixture {

    val conductor = new Conductor

    import conductor._

    val promise = new Promise[ReturnType]()
    val entry = log.append(AnyTerm, AnyCommand, Array("arg1"), Some(promise))

    thread("reader") {
      Await.result(promise) should be(None)
    }

    thread("writer") {
      entry.apply(delegate)
    }
  }
}
