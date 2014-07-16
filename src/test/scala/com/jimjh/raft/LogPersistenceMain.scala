package com.jimjh.raft

import java.util.Properties

import com.jimjh.raft.log.LogEntry
import scala.util.{Success, Failure}

/** Reads log entries from disk.
  *
  */
object LogPersistenceMain extends PersistenceComponent {

  val props = {
    val props = new Properties()
    props.setProperty("node.id", "localhost:8082")
    props.setProperty("data.dir", "/tmp/raft")
    props
  }

  override val persistence: LogPersistenceMain.Persistence = new Persistence(props)

  def main(args: Array[String]) = {
    try {
      persistence.readLog[(String, LogEntry)].foreach(println(_))
    } finally persistence.close()
  }
}
