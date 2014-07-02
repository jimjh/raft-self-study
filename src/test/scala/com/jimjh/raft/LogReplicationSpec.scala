package com.jimjh.raft

import com.jimjh.raft.spec.Harness
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar
import org.scalatest.{Matchers, FlatSpec}
import scala.util.Random

/** Specs for the Log Replication feature.
  *
  * @author Jim Lim - jim@jimjh.com
  */
class LogReplicationSpec
  extends FlatSpec
  with Matchers
  with Eventually
  with SpanSugar {

  val rand = new Random

  // number of milliseconds to wait
  val pause = 10000

  // number of raft servers
  val count = 3

  trait Fixture {
    val command = rand.nextString(10)
    val rafts = new Harness().withApp(app).build(count) // start raft servers

    def waitForLeader(): RaftServer = {
      eventually(timeout(scaled(pause milliseconds))) {
        // rafts.find(_.isLeader) should be ('defined)
        rafts.find(_.isLeader) match {
          case Some(leader) => leader
          case None => fail("No leader found.")
        }
      }
    }
  }

  def app() = {
    new Application() {
      override def apply(cmd: String, args: Seq[String]): ReturnType = {
        None
      }
    }
  }

  it should "copy commands to all followers" in new Fixture {

    val leader = waitForLeader()

    // send command to leader
    leader.consensusService.apply(command, Nil)

    // ensure that all followers should have copied that command
    eventually(timeout(scaled(pause milliseconds))) {
      rafts.foreach {
        raft =>
          raft.consensusService.lastLogIndex.elem.cmd should equal(command)
      }
    }
  }

  // TODO followers should truncate their own logs
  // TODO the leader should increment its commit index after replicating the entry to a majority of servers
  // TODO the leader should not increment its commit index before replicating the entry to a majority of servers
  // TODO followers should update their commit index from the leader
}
