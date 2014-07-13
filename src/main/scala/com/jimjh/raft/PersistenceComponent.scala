package com.jimjh.raft

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}
import java.nio.file.StandardOpenOption._
import java.nio.file.{Files, Paths}
import java.util.Properties

import org.apache.commons.io.IOUtils

/** Provides an implementation of `Persistence` for persisting node state and the log.
  *
  * @author Jim Lim - jim@jimjh.com
  */
trait PersistenceComponent {

  val persistence: Persistence

  // #injectable

  /** Serializes and de-serializes node state and the log to disk.
    *
    * Not thread-safe. Synchronization should be done elsewhere. If I need this class to be thread-safe,
    * I have done something very wrong.
    *
    * @param _props configuration options for persistence
    */
  class Persistence(_props: Properties) {

    // TODO add validation, CRC32
    Files.createDirectories(dir)

    private[this] val _nodePath = dir.resolve(Persistence.NodeFile)

    private[this] val _logPath = dir.resolve(Persistence.LogFile)

    private[this] val _logStream = newLogStream

    /** Serializes `o` to `_nodePath`.
      *
      * @param o object to be serialized
      */
    def writeNode(o: Serializable) {
      var output: ObjectOutputStream = null
      try {
        val fos = Files.newOutputStream(_nodePath, CREATE, WRITE, TRUNCATE_EXISTING)
        output = new ObjectOutputStream(fos)
        output writeObject o
      } finally {
        IOUtils closeQuietly output
      }
    }

    /** De-serializes an object from `_nodePath`.
      *
      * @tparam T type of object to return
      * @return de-serialized object, casted to `T`
      */
    def readNode[T]: Option[T] = {
      var input: ObjectInputStream = null
      try {
        val fis = Files.newInputStream(_nodePath, READ)
        input = new ObjectInputStream(fis)
        Some(input.readObject().asInstanceOf[T])
      } catch {
        case ioe: IOException => None
      } finally {
        IOUtils closeQuietly input
      }
    }

    def appendLog(o: Serializable) {
      _logStream writeObject o
      _logStream.flush()
    }

    def closeLog() {
      IOUtils closeQuietly _logStream
    }

    private[this] def dir =
      Paths.get(_props.getProperty("data.dir"), _props.getProperty("node.id"))

    private[this] def newLogStream =
      new ObjectOutputStream(Files.newOutputStream(_logPath, CREATE, WRITE, TRUNCATE_EXISTING))
  }

  object Persistence {
    val NodeFile = "node.bin"
    val LogFile = "log.bin"
  }
}
