package com.jimjh.raft

import com.jimjh.raft.spec.UnitSpec

/** Specs for the [[ConsensusServiceComponent]].
  *
  * @author Jim Lim - jim@jimjh.com
  */
class ElectionSpec extends UnitSpec {
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
