package com.jimjh.raft.leadership

import com.jimjh.raft.log.LogEntry
import com.typesafe.scalalogging.slf4j.Logger

import scala.collection.mutable

/** Tracks the match index of each follower and updates the commit index.
  *
  * @author Jim Lim - jim@jimjh.com
  */
trait Leader {

  /** Ordered set of match indexes for each peer, stored as `(id, match index)`. */
  private[this] val _matches = new mutable.TreeSet[Match]()

  /** @return current term */
  def termNum: Long

  /** @return current commit index */
  def commit: Long

  def commit_=(index: Long): Unit

  /** @return last log entry on the log */
  def lastLogEntry: LogEntry

  /** @return required number of votes/replication acks */
  def requiredMajority: Int

  protected def logger: Logger

  /** Tells the leader that a peer's match index has been advanced.
    *
    * @param term leadership term number
    * @param id ID of the node this proxy represents
    * @param index new match index
    */
  def updateMatch(term: Long, id: String, index: Long) = synchronized {
    term == termNum match {
      case true =>
        // update match index for given peer
        _matches.find(_.id == id) match {
          case Some(p) =>
            _matches.remove(p)
          case None =>
        }
        _matches.add(Match(id, index))
        // minus one for the leader
        val req = requiredMajority - 1
        // advance commit
        val majority = _matches.takeRight(req)
        if (majority.size >= req) this.commit = majority.firstKey.index
      case false =>
        logger.warn(s"Ignoring #updateMatchIndex for expired term: $term.")
    }
  }

  def resetMatch() = synchronized {
    _matches.clear()
  }

  case class Match(id: String, index: Long) extends Ordered[Match] {
    override def compare(other: Match) = index compareTo other.index
  }

}
