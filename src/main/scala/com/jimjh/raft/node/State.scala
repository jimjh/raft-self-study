package com.jimjh.raft.node

/** Defines various possible states for [[Node]]. */
object State extends Enumeration {
  type State = Value
  val Fol, Cand, Ldr = Value
}