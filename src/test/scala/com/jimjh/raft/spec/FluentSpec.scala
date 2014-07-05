package com.jimjh.raft.spec

import org.scalatest.concurrent.{Conductors, Eventually}
import org.scalatest.time.SpanSugar
import org.scalatest.{Matchers, WordSpec}

class FluentSpec
  extends WordSpec
  with Matchers
  with Eventually
  with SpanSugar
  with Conductors