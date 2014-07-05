package com.jimjh.raft.log

import com.jimjh.raft.{Application, ReturnType}
import com.jimjh.raft.spec.FluentSpec

import scala.concurrent.Await
import scala.concurrent.promise
import scala.concurrent.duration.Duration

/** Specs for [[com.jimjh.raft.log.LogEntry]].
  *
  * These tests are very specific to my implementation of RAFT, and may not be necessary to ensure correctness of the
  * overall algorithm. They are here to help me check that I implemented my design correctly.
  *
  * @author Jim Lim - jim@jimjh.com
  */
class LogEntrySpec extends FluentSpec {

  "A sentinel" when {
    "created" should {
      "have a prev ptr that points to itself" in {
        val sentinel = LogEntry.sentinel
        sentinel.prev should be theSameInstanceAs sentinel
      }

      "be a different instance each time" in {
        val sentinel = LogEntry.sentinel
        sentinel shouldNot be theSameInstanceAs LogEntry.sentinel
      }
    }
  }

  "A log entry" when {
    val term = 12
    val index = 15
    val cmd = "any.command"
    val args = "any.arg"::Nil

    "created" should {
      val entry = LogEntry(term, index, cmd, args)

      "have the given term, index, command, and args" in {
        entry.term should be (term)
        entry.index should be (index)
        entry.cmd should be (cmd)
        entry.args should be (args)
      }

      "be last" in {
        entry should be ('last)
      }
    }

    "appended" should {
      val first = LogEntry(term, index, cmd)
      val second = first << (term, index+1, cmd)

      "be last" in {
        first shouldNot be ('last)
        second should be ('last)
      }

      "not have nextF completed" in {
        first.nextF should be ('completed)
        second.nextF shouldNot be ('completed)
      }

      "point back to its predecessor" in {
        val next = Await.result(first.nextF, Duration.Inf)
        next should be (second)
        second.prev should be (first)
      }
    }

    "compared" should {
      "use index" in {
        val a = LogEntry(term, 100, cmd)
        val b = LogEntry(term, 167, cmd)

        (a compare b) should be (-1)
        (b compare a) should be (1)
        (a compare a) should be (0)
      }
    }

    "applied" should {
      "fulfill promises, if any" in {
        object delegate extends Application {
          override def apply(cmd: String, args: Seq[String]): Option[Any] = None
        }

        val conductor = new Conductor
        import conductor._

        val p = promise[ReturnType]()
        val entry = LogEntry(term, 1, cmd, args, Some(p))

        thread("reader") {
          Await.result(p.future, Duration.Inf) should be(None)
        }

        thread("writer") {
          entry(delegate)
        }
      }
    }
  }
}
