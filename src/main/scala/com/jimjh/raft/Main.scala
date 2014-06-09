package com.jimjh.raft

import com.twitter.finagle.{Http, Service}
import com.twitter.util.{Await, Future}
import org.jboss.netty.handler.codec.http.{HttpResponseStatus, DefaultHttpResponse, HttpResponse, HttpRequest}

object Main {
  def main(args: Array[String]) {
    println("running main ...")
    val server = Http.serve(":8080", RaftServer.consensusService)
    println("launching server ...")
    Await.ready(server)
  }
}
