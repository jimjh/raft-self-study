package com.jimjh.raft

import java.io.ObjectOutputStream
import java.nio.channels.Channels
import java.nio.file.StandardOpenOption._
import java.nio.file.{Files, Paths}

import org.apache.commons.io.IOUtils

object PerfMain {

  def main(args: Array[String]) {
    val t1 = System.currentTimeMillis()
    append1()
    val t2 = System.currentTimeMillis()
    println(s"append1 took ${t2 - t1} ms")
    val t3 = System.currentTimeMillis()
    append2()
    val t4 = System.currentTimeMillis()
    println(s"append1 took ${t4 - t3} ms")
  }

  def append1() {
    val path = Paths.get("/tmp/x")
    val channel = Files.newByteChannel(path, CREATE, WRITE, TRUNCATE_EXISTING)
    var output: ObjectOutputStream = null
    try {
      output = new ObjectOutputStream(Channels.newOutputStream(channel))
      for (i <- 1 until 10000) {
        output.writeObject(i)
        output.flush()
      }
    } finally {
      IOUtils.closeQuietly(output)
    }
  }

  def append2() {
    val path = Paths.get("/tmp/x")
    var output: ObjectOutputStream = null
    try {
      output = new ObjectOutputStream(Files.newOutputStream(path, CREATE, WRITE, TRUNCATE_EXISTING))
      for (i <- 1 until 10000) {
        output.writeObject(i)
        output.flush()
      }
    } finally {
      IOUtils.closeQuietly(output)
    }
  }
}
