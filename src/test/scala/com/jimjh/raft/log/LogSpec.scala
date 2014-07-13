package com.jimjh.raft.log

import com.jimjh.raft._
import com.jimjh.raft.rpc.Entry
import com.jimjh.raft.spec.FluentSpec

/** Specs for [[LogComponent.Log]]
  *
  * @author Jim Lim - jim@jimjh.com
  */
class LogSpec
  extends FluentSpec
  with LogComponent {

  // dummy application that just prepends commands to a sequence
  class Delegate extends Application {
    var history = List[String]()

    override def apply(cmd: String, args: Seq[String]): Option[Any] = {
      history = cmd +: history
      None
    }
  }

  def delegate = new Delegate

  val AnyTerm = 5
  val AnyCommand = "any.command"
  val AnyArgs = "arg1" :: Nil

  override val log = null
  override val persistence = ???
  // FIXME
  override val logger = null // FIXME

  "An empty log" when {

    "appending a new log entry" should {
      val log = new Log(delegate).start()
      log.append(AnyTerm, AnyCommand, AnyArgs)

      "increment the log index" in {
        log.lastIndex should be(1)
      }

      "create the proper log entry" in {
        log.last.cmd should be(AnyCommand)
        log.last.args should be(AnyArgs)
      }

      "not apply the command (since it has not been committed)" in {
        log.lastApplied should be(0)
      }
    }

    "appending 99 new log entries" should {
      val log = new Log(delegate).start()
      (1 until 100).foreach(log.append(_, AnyCommand, AnyArgs))

      "increment the log index" in {
        log.lastIndex should be(99)
      }

      "create the proper log entry" in {
        log.last.cmd should be(AnyCommand)
        log.last.args should be(AnyArgs)
      }

      "not apply the commands (since they have not been committed)" in {
        log.lastApplied should be(0)
      }
    }
  }

  "A populated log" when {
    val PopulateCount = 100
    val Term = 1

    val del = new Delegate

    def populatedLog = {
      val log = new Log(del).start()
      (1 until PopulateCount + 1).foreach(i => log.append(Term, s"command#$i"))
      log
    }

    "committed" should {
      // here, "committed" means raising the commit index
      val CommitIndex = 93L
      val log = populatedLog
      log.commit = CommitIndex

      "apply (asynchronously) commands up till the commit index" in {
        eventually(timeout(scaled(1000 milliseconds))) {
          log.lastApplied should be(CommitIndex)
          del.history.length should be(CommitIndex)
          del.history.head should be(s"command#$CommitIndex")
        }
      }

      "update the commit index" in {
        log.commit should be(CommitIndex)
      }

      "should not decrease the commit index" in {
        log.commit = 9L
        log.commit should be(CommitIndex)
      }
    }

    "stopped" should {
      val log = populatedLog
      "stop without errors" in {
        log.stop()
      }
    }

    "using #findLast" should {
      val log = populatedLog

      "find rightmost entry with matching index and term" in {
        val opt = log.findLast(54, Term)
        opt should be('defined)
        opt.get.index should be(54)
        opt.get.term should be(Term)
      }

      "not find false positives" in {
        log.findLast(54, 2) shouldNot be('defined)
      }
    }

    "using #appendEntries" should {
      // given a populated log
      val log = populatedLog
      val tail = log.last
      val from = log.last.prev.prev.prev

      // when appended/truncated
      val entries = List(Entry("x"), Entry("y"), Entry("z"))
      val last = log.appendEntries(0, entries, from)

      "append all of the given entries" in {
        last should be('last)
        last.cmd should be("z")
      }

      "truncate from given starting entry" in {
        last.index should be(tail.index)
      }

      "organize entries with proper indexes" in {
        var ptr = last
        var index = ptr.index
        while (ptr != ptr.prev) {
          ptr.index should be(index)
          ptr = ptr.prev
          index = index - 1
        }
      }
    }

    "append and advance a given log index w. a future" in {
      import scala.concurrent.ExecutionContext.Implicits.global

      val log = populatedLog
      val last = log.last
      var fulfilled = false
      last.nextF.map { _ => fulfilled = true}

      fulfilled should be(right = false)
      log.append(0, "any.cmd")
      eventually {
        fulfilled should be(right = true)
      }
    }

    "appended and truncated" should {
      val log = populatedLog
      val last = log.last
      val prev = last.prev.prev.prev
      val entry = log.append(0, "any.cmd", Nil, None, prev)

      "pass along a TruncatedLogException" in {
        import scala.concurrent.ExecutionContext.Implicits.global

        @volatile
        var fulfilled = 0
        last.nextF
          .map { case e => fulfilled = 1}
          .onFailure { case e: TruncatedLogException => fulfilled = 2}

        eventually {
          fulfilled should be(2)
        }
      }

      "actually truncate" in {
        entry.index should be(last.index - 2)
        entry should be('last)
      }
    }
  }

  "An isolated log" when {

    val del = new Delegate
    val log = new Log(del).start()

    "created" should {

      "allow concurrent appending and committing" in {
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
          log.commit = count // commit all
          waitForBeat(2)
          log.commit = 2 * count // commit all
        }

        whenFinished {
          eventually {
            log.lastApplied should be(2 * count)
            del.history.length should be(2 * count)
          }
        }
      }
    }
  }
}
