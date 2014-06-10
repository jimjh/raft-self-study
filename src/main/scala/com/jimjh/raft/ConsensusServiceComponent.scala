package com.jimjh.raft

import com.twitter.finagle.Service
import com.twitter.util.Future
import org.jboss.netty.handler.codec.http.{HttpResponse, HttpRequest, HttpResponseStatus, DefaultHttpResponse}

/** Wrapper for a Consensus Service.
  *
  * It looks odd because I am trying to use the Cake Pattern.
  *
  * @author Jim Lim - jim@quixey.com
  */
trait ConsensusServiceComponent {

  val consensusService: ConsensusService

  /** Responds to RPCs from other servers.
    *
    */
  class ConsensusService extends Service[HttpRequest, HttpResponse] {
    override def apply(req: HttpRequest): Future[HttpResponse] =
    // TODO implement consensus service
      Future.value(new DefaultHttpResponse(
        req.getProtocolVersion, HttpResponseStatus.OK))
  }

}
