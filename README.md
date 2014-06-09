# RAFT

This is a self-study project. My goals for this project are

- learn to write better networking code
- learn to write better Scala code
- develop a detailed understanding of RAFT

## RPC

All Remote Procedure Calls will be implemented using Thrift.

## Classes

### Client

Makes RPCs to the server to execute some command on the state machine. Details
TBD.

### Server

Each server will be implemented using a state machine that governs the
transitions between the follower, leader, and candidate states. Servers have
the following attributes:

  - `currentTerm:long` (persistent)
  - votedFor (persistent) - do we need one for each term?
  - commitIndex
  - state - one of `FOLLOWER`, `LEADER`, or `CANDIDATE`
  - `nextIndex[]`
  - `matchIndex[]`
  - log

### Log

The log is responsible for

  - file I/O,
  - compaction (snapshotting),
  - application,
  - flushing,
  - recovery etc.

Attributes include

  - `lastApplied:long` (volatile/persistent, depends on snapshot),
  - `delegate:App`

Commands are applied by forwarding them to the delegate.

### App

`App` defines an application interface. Applications implement this interface
to receive committed commands from the log.

On success, the `lastApplied` counter is incremented. If the application raises
and exception, the server will be terminated.

`public void apply(cmd:str, args:[str])`

As documented in the RAFT paper, it's up to the application to prevent
duplicate executions of the same command _e.g._ assign unique serial numbers to
each command and ignore command that have been executed.

### Timer

Deals with timeouts.

## Testing

Maybe there is a way to provide a proxy for the sockets so we can simulate
various network issues _e.g._ lossy, partitions.

## Schedule

- Phase 1: Skeleton classes, RPCs
- Phase 2: Timeouts, Leadership Election
- Phase 3: Log Replication and Application
- Phase 4: Membership Changes
- Phase 5: Log Compaction (Snapshotting)

## Reference

- Onagro, D., Ousterhout, J. [_In Search of an Understandable Consensus Algorithm_][raft], Stanford University, 2014

  [raft]: https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf
