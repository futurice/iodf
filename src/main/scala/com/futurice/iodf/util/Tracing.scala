package com.futurice.iodf.util

import java.util.logging.Level

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

/**
  * Created by arau on 4.7.2017.
  */
object Tracing {

  val l = LoggerFactory.getLogger(getClass)

  var openItems = 0
  var traces : Option[mutable.Map[Any, Exception]] = None

  def opened(trace:Any) = synchronized {
    traces match {
      case Some(tr) => tr += (trace -> new RuntimeException("leaked reference"))
      case None =>
    }
    openItems += 1
  }
  def closed(trace:Any) = synchronized {
    traces match {
      case Some(tr) =>
        tr.remove(trace)
      case None =>
    }
    openItems -= 1
  }

  def trace[T](f : => T) : T = {
    // FIXME: not thread safe! Use threadlocal!
    val tracesBefore = traces
    traces = Some(new mutable.HashMap[Any, Exception]())
    try {
      f
    } finally {
      if (traces.get.size > 0) {
        l.error(traces.get.size + " resouces leaked.")
        traces.get.take(1).map { t =>
          l.error(t._1 + " leaked!", t._2)
        }
      }
      traces = tracesBefore
    }
  }
}
