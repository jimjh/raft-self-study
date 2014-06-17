package com.jimjh.raft

import org.scalatest.{FlatSpec, Matchers}

/** Specs for the [[ConsensusServiceComponent]].
  *
  * @author Jim Lim - jim@jimjh.com
  */
class ConsensusServiceSpec
  extends FlatSpec
  with Matchers {
  // FOLLOWER
  it should "timeout and start an election"
  it should "not timeout if it receives heartbeats"
  it should "discover new terms from AppendEntries"
  it should "discover new terms from RequestVote"
  it should "vote for the first candidate that is up to date"
  it should "vote for candidates that are not up to date"
  it should "vote for only one candidate during each given term"

  // CANDIDATE
  // TODO

  // LEADER
  // TODO
}
