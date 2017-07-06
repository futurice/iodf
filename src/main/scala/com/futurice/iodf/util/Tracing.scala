package com.futurice.iodf.util

import java.util.logging.Level

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

class Tracing() {
  val l = LoggerFactory.getLogger(getClass)

  val open = mutable.Map[Any, Exception]()
  val _closed = mutable.WeakHashMap[Any, Exception]()
  var openItems = 0

  def opened(trace:Any) = synchronized {
    open += (trace -> new RuntimeException("leaked reference"))
    openItems += 1
  }
  def closed(trace:Any) = synchronized {
    open.remove(trace) match {
      case Some(v) =>
        _closed += (trace -> new RuntimeException("was closed first here"))
      case None =>
        l.error("" + trace + " was deleted twice! ", new RuntimeException("here"))
        _closed.get(trace) match {
          case Some(e) =>
            l.error( trace + " was first deleted here", e)
          case None =>
            l.error("either non-traced item was reported closed or we forgot deletion?")
        }
    }
    openItems -= 1
  }
  def report = {
    if (open.size > 0) {
      l.error(open.size + " resouces leaked.")
      open.take(2).map { t =>
        l.error(t._1 + " leaked!", t._2)
      }
    }
  }
}

/**
  * Created by arau on 4.7.2017.
  */
object Tracing {

  val l = LoggerFactory.getLogger(getClass)

  var openItems = 0

  var tracing : Option[Tracing] = None

  def opened(trace:Any) = synchronized {
    tracing match {
      case Some(tr) => tr.opened(trace)
      case None =>
    }
    openItems += 1
  }
  def closed(trace:Any) = synchronized {
    tracing match {
      case Some(tr) => tr.closed(trace)
      case None =>
    }
    openItems -= 1
  }
  def lightTrace[T](f : => T) : T = {
    val openItemsBefore = openItems
    try {
      f
    } finally {
      if (openItems != openItemsBefore) {
        l.error("The number of opened items changed: " + openItemsBefore + " -> " + openItems)
      }
    }


  }

  def trace[T](f : => T) : T = {
    // FIXME: not thread safe! Use threadlocal!
    val tracesBefore = tracing
    tracing = Some(new Tracing())
    try {
      f
    } finally {
      tracing.get.report
      tracing = tracesBefore
    }
  }
}
