package com.jimjh.raft

import com.jimjh.raft.spec.{UnitSpec, Harness}

import scala.util.Random

/** Specs for the Log Replication feature.
  *
  * @author Jim Lim - jim@jimjh.com
  */
class LogReplicationSpec extends UnitSpec {

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
        rafts.find(_.isLeader) match {
          case Some(leader) => leader
          case None => fail("No leader found.")
        }
      }
    }
  }

  def app() = new Application() {
    override def apply(cmd: String, args: Seq[String]): ReturnType = {
      None
    }
  }

  it should "copy commands to all followers" in new Fixture {
    // given leader
    val leader = waitForLeader()

    // when a command is given
    leader.consensus.apply(command, Nil)

    // then all followers should receive it
    eventually(timeout(scaled(pause milliseconds))) {
      rafts.foreach { _.log.last.cmd should equal(command) }
    }
  }

  it should "increment the leader's commit index, eventually" in new Fixture {
    // given leader
    val leader = waitForLeader()

    // when a command is given
    (1 until 100).foreach(i => leader.consensus.apply(s"command $i", Nil))

    // then all followers should receive it
    eventually(timeout(scaled(pause milliseconds))) {
      leader.commit should be (99)
    }
  }

  it should "increment all follower's commit indexes, eventually" in new Fixture {
    // given leader
    val leader = waitForLeader()

    // when a command is given
    (1 until 100).foreach(i => leader.consensus.apply(s"command $i", Nil))

    // then all followers should receive it
    eventually(timeout(scaled(pause milliseconds))) {
      rafts.foreach { _.commit should equal(99) }
    }
  }

  // TODO followers should truncate their own logs
  // TODO the leader should not increment its commit index before replicating the entry to a majority of servers
}
