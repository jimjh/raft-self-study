# RAFT

This is a self-study project. My goals for this project are

- learn to write better networking code
- learn to write better Scala code
- develop a detailed understanding of RAFT

To get started, use

    $ sbt compile

I use the Cake Pattern throughout the project for dependency injection. The assembly module lives in
`src/scala/com/jimjh/raft/package.scala`, which might be a good place to start exploring the code.

## RPC

All Remote Procedure Calls are implemented using [Finagle][finagle] and [Thrift][thrift]. The Scala classes may be
generated using [Scrooge][scrooge], as follows

    $ sbt scroogeGen

## Rough Design

Here's a simplified description of the various classes and traits I intend to have. In practice, there will be a
proliferation of traits and classes to enable dependency injection. Refer to the ScalaDoc for a detailed and up-to-date
description of each package, class, and trait.

Users of this library are expected to

- provide an application that implements the `Application` trait,
- provide a logger that implements the slf4j api, and
- launch instances of `RaftServer`.

ScalaDocs may be generated using

    $ sbt doc
    
The [package overview][package.scala] might be a good place to start.

### Client

The client makes RPCs to the server to execute some command on the state machine. Details TBD.

### Server

Each server will be implemented using a state machine that governs the transitions between the follower, leader, and
candidate states. Servers have the following attributes:

  - `currentTerm:long` (persistent)
  - votedFor (persistent) - do we need one for each term?
  - commitIndex
  - state - one of `FOLLOWER`, `LEADER`, or `CANDIDATE`
  - `nextIndex[]`
  - `matchIndex[]`
  - log
  
Its implementation shall be divided into a `ClientService` (handles RPCs from clients) and a `ConsensusService` (handles
RPCs between RAFT nodes.)

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

### Application

`Application` defines an application interface. Applications implement this interface to receive committed commands from
the log.

On success, the `lastApplied` counter is incremented. If the application raises an exception, the server will be
terminated.

    def apply(cmd: String, args: Array[String])

As documented in the RAFT paper, it's up to the application to prevent duplicate executions of the same command _e.g._
assign unique serial numbers to each command and ignore commands that have been executed.

### ElectionTimer

Triggers election timeouts on the server.

### HeartBeat

Triggers periodic heartbeats on the server.

## Testing

An effort will be made to use dependency injection across the library. This will allow tests to provide mocks that can
be used to simulate various network failures.

To run the tests, use

    $ sbt test

## Schedule

- Phase 1: Skeleton classes, RPCs
- Phase 2: Timeouts, Leadership Election
- Phase 3: Log Replication and Application
- Phase 4: Membership Changes
- Phase 5: Log Compaction (Snapshotting)

## Reference

- Onagro, D., Ousterhout, J. [_In Search of an Understandable Consensus Algorithm_][raft], Stanford University, 2014

  [raft]: https://ramcloud.stanford.edu/wiki/download/attachments/11370504/raft.pdf
  [finagle]: http://twitter.github.io/finagle/
  [thrift]: http://thrift.apache.org/
  [scrooge]: http://twitter.github.io/scrooge/
  [package.scala]: target/scala-2.10/api/index.html#com.jimjh.raft.package