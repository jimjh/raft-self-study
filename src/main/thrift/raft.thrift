namespace java com.jimjh.raft.rpc

struct Vote {
  1: i64 term     # current term identifying the election
  2: bool granted # true means candidate received vote
}

service RaftConsensusService {
  Vote requestVote(1: i64 term,           # candidate's term
                   2: string candidateId, # ID of candidate requesting vote
                   3: i64 lastLogIndex,   # index of candidate's last log entry
                   4: i64 lastLogTerm)    # term of candidate's last log entry
}