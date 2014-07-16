package com.jimjh.raft

import java.io.{Closeable, IOException, ObjectInputStream, ObjectOutputStream}
import java.nio.file.StandardOpenOption._
import java.nio.file.{Files, Paths}
import java.util.Properties

import org.apache.commons.io.IOUtils

import scala.util.Try

/** Provides an implementation of `Persistence` for persisting node state and the log.
  *
  * @author Jim Lim - jim@jimjh.com
  */
trait PersistenceComponent {

  val persistence: Persistence // #injectable

  /** Serializes and de-serializes node state and the log to disk.
    *
    * Not thread-safe. Synchronization should be done elsewhere. If I need this class to be thread-safe,
    * I have done something very wrong.
    *
    * @param _props configuration options for persistence
    */
  class Persistence(_props: Properties) extends Closeable {

    // TODO add validation, CRC32
    Files.createDirectories(dir)

    private[this] val _nodePath = dir.resolve(Persistence.NodeFile)

    private[this] val _logPath = dir.resolve(Persistence.LogFile)

    private[this] val _logInput = openLogInput
    private[this] val _logOutput = openLogOutput

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

    /** Appends a single log entry to [[Persistence.LogFile]].
      *
      * @param o object to be serialized
      */
    def appendLog(o: Serializable) {
      _logOutput writeObject ("a", o)
      _logOutput.flush()
    }

    /** De-serializes log entries from [[Persistence.LogFile]].
      *
      * How I might use it
      * {{{
      *   loop
      *     read log entry
      *     if None, break
      *     modify previous log entry
      *   end
      * }}}
      *
      * @tparam T type of object to return
      * @return de-serialized object, casted to `T`
      */
    def readLog[T]: Iterator[T] =
      new Iterator[T] {
        var _next: Try[T] = tryNext

        override def hasNext = _next.isSuccess

        override def next() = {
          val curr = _next.get
          _next = tryNext
          curr
        }

        private[this] def tryNext =
          _logInput flatMap (stream => Try(stream.readObject().asInstanceOf[T]))
      }

    /** Marks a truncation in the log.
      *
      * @param index index of the entry whose successor was cut
      */
    def truncateLog(index: Long) = {
      _logOutput writeObject ("t", index)
      _logOutput.flush()
    }

    def close() {
      _logInput.map(IOUtils closeQuietly _)
      IOUtils closeQuietly _logOutput
    }

    private[this] def dir =
      Paths.get(_props.getProperty("data.dir"), _props.getProperty("node.id"))

    private[this] def openLogInput = Try {
      new ObjectInputStream(Files.newInputStream(_logPath, READ))
    }

    private[this] def openLogOutput = // let any IOExceptions bubble up
      new ObjectOutputStream(Files.newOutputStream(_logPath, CREATE, APPEND))
  }

  object Persistence {
    val NodeFile = "node.bin"
    val LogFile = "log.bin"
  }
}
