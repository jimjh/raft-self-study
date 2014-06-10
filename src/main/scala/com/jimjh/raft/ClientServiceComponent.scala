package com.jimjh.raft

import com.twitter.finagle.Service
import org.jboss.netty.handler.codec.http.{HttpResponseStatus, DefaultHttpResponse, HttpRequest, HttpResponse}
import com.twitter.util.Future

/** Wrapper for a Client Service.
  *
  * This looks odd because I am trying to learn the Cake Pattern.
  *
  * @author Jim Lim - jim@quixey.com
  */
trait ClientServiceComponent {

  val clientService: ClientService

  /** Responds to RPCs from Clients.
    *
    */
  class ClientService extends Service[HttpRequest, HttpResponse] {
    override def apply(req: HttpRequest): Future[HttpResponse] =
    // TODO implement client service
      Future.value(new DefaultHttpResponse(
        req.getProtocolVersion, HttpResponseStatus.OK))
  }

}
