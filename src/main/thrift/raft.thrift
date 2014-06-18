namespace java com.jimjh.raft.rpc

struct Vote {
  1: i64 term     # current term identifying the election
  2: bool granted # true means candidate received vote
}

struct Entry {
  1: string cmd,       # command
  2: list<string> args # arguments
}

service RaftConsensusService {
  Vote requestVote(1: i64 term,           # candidate's term
                   2: string candidateId, # ID of candidate requesting vote
                   3: i64 lastLogIndex,   # index of candidate's last log entry
                   4: i64 lastLogTerm)    # term of candidate's last log entry

  # FIXME what does "for leader to update iteself" mean?
  bool appendEntries(1: i64 term,         # leader's term
                     2: string leaderId,  # leader's ID
                     3: i64 prevLogIndex, # index of log entry immediately preceding new ones
                     4: i64 prevLogTerm,  # term of prevLogIndex
                     5: list<Entry> entries, # list of new log entries
                     6: i64 leaderCommit) # leader's commitIndex
}