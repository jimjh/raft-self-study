package com.jimjh.raft.log

/** Raised in [[com.jimjh.raft.log.LogEntry.nextF]] on truncation. */
class TruncatedLogException extends Exception
