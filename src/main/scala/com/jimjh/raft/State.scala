package com.jimjh.raft

/** Defines various possible states for the ConsensusService. */
object State extends Enumeration {
  type State = Value
  val Follower, Candidate, Leader = Value
}