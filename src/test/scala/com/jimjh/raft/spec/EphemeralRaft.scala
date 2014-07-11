package com.jimjh.raft.spec

import java.util.Properties

import com.jimjh.raft._

/** An implementation of Raft that doesn't persist anything to disk. This could not more suitable for some
  * integration tests. Part of my reason for creating this class is to test my understanding of dependency injection
  * in Scala.
  */
class EphemeralRaft(_delegate: Application,
                    _props: Properties)
  extends RaftServer(_delegate, _props)
  with EphemeralComponent {
  override val persistence = new Ephemeral(_props)
}

trait EphemeralComponent extends PersistenceComponent {
  class Ephemeral(_props: Properties) extends Persistence(_props) {
    override def write(o: Serializable) {} // do nothing
    override def read[T]: Option[T] = None
  }
}