package com.jimjh.raft

import java.nio.file.{Files, Path}
import java.util.Properties

import com.jimjh.raft.log.LogEntry
import com.jimjh.raft.spec.UnitSpec
import org.apache.commons.io.FileUtils

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Random

/** Specs for [[com.jimjh.raft.PersistenceComponent]]
  *
  * @author Jim Lim - jim@jimjh.com
  */
class PersistenceSpec
  extends UnitSpec {

  var dir: Path = null
  val rand = new Random

  before {
    dir = Files createTempDirectory "persistence-spec"
  }

  after {
    FileUtils.deleteDirectory(dir.toFile)
  }

  def randomString(n: Int) = rand.alphanumeric.take(n).mkString

  trait Fixture extends PersistenceComponent {
    val nodeId = randomString(10)
    val props = new Properties()
    props setProperty("data.dir", dir.toString)
    props setProperty("node.id", nodeId)
  }

  it should "persist and rebuild" in new Fixture {
    val persist = new Persistence(props)

    val term = rand.nextInt(1000)
    persist.appendLog(LogEntry(term, 1, "cmd.1"))
    persist.appendLog(LogEntry(term, 2, "cmd.2"))
    persist.appendLog(LogEntry(term, 3, "cmd.3"))
    persist.close()

    val recover = new Persistence(props)
    val root = recover.rebuildLog

    val one = Await.result(root.nextF, 0 nanos)
    one.cmd should be ("cmd.1")
    val two = Await.result(one.nextF, 0 nanos)
    two.cmd should be ("cmd.2")
    val thr = Await.result(two.nextF, 0 nanos)
    thr.cmd should be ("cmd.3")
  }
}
