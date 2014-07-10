package com.jimjh.raft

import java.io.{IOException, ObjectInputStream, ObjectOutputStream}
import java.nio.file.StandardOpenOption._
import java.nio.file.{Files, Paths}
import java.util.Properties

import org.apache.commons.io.IOUtils

/** Provides an implementation of `Persistence`, which serializes and de-serializes a tuple.
  *
  * @author Jim Lim - jim@jimjh.com
  */
trait PersistenceComponent {

  val persistence: Persistence // #injectable

  /** Serializes and de-serializes a given tuple to disk.
    *
    * @param _props configuration options for persistence
    */
  class Persistence(_props: Properties) {

    private[this] val _path = nodePath

    /** Serializes `o` to `_path`.
      *
      * @param o object to be serialized
      */
    def write(o: Serializable) {
      var output: ObjectOutputStream = null
      try {
        val fos = Files.newOutputStream(_path, CREATE, WRITE, TRUNCATE_EXISTING)
        output = new ObjectOutputStream(fos)
        output.writeObject(o)
      } finally {
        IOUtils.closeQuietly(output)
      }
    }

    /** De-serializes an object from `_path`.
      *
      * @tparam T type of object to return
      * @return de-serialized object, casted to `T`
      */
    def read[T]: Option[T] = {
      var input: ObjectInputStream = null
      try {
        val fis = Files.newInputStream(_path, READ)
        input = new ObjectInputStream(fis)
        Some(input.readObject().asInstanceOf[T])
      } catch {
        case ioe: IOException => None
      } finally {
        IOUtils.closeQuietly(input)
      }
    }

    private[this] def nodePath = Paths.get(_props getProperty "data.dir", Persistence.NodeFile)
  }

  object Persistence {
    val NodeFile = "node.bin"
  }
}
