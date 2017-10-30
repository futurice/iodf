package com.futurice.iodf.util

import java.util.logging.Level

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

class Tracing() {
  val l = LoggerFactory.getLogger(getClass)

  val open = mutable.Map[Any, Exception]()
  val _closed = mutable.WeakHashMap[Any, (Exception, Exception)]()
  var openItems = 0

  def opened(trace:Any) = synchronized {
    open += (trace -> new RuntimeException("created here!"))
    openItems += 1
  }
  def closed(trace:Any) = synchronized {
    open.remove(trace) match {
      case Some(err) =>
        _closed += (trace -> (err, new RuntimeException("was closed first here")))
      case None =>
        _closed.get(trace) match {
          case Some(e) =>
            l.error( trace.getClass + " was created here", e._1)
            l.error( "then " + trace.getClass + " was deleted here", e._2)
            /*            l.error( "was created here", e._1)
            l.error( "then deleted here", e._2)*/
          case None =>
            l.error("either non-traced item was reported closed or we forgot deletion?")
        }
        l.error("and then " + trace.getClass + " was deleted second time! ", new RuntimeException("here"))
    }
    openItems -= 1
  }
  def report = {
    if (open.size > 0) {
      open.groupBy(_._1.getClass).toArray.sortBy(-_._2.size).foreach { case (typ, errs) =>
        l.error(errs.size + " instances of " + typ + " leaked", errs.head._2);
        open.filter(o => o._1 match {
          case Ref(e) if errs.exists(_._1 == e) =>
            true
          case _ =>
            false
        }).take(2).foreach { o =>
          l.error("open references created here ", o._2)
        }
      }
      l.error(open.size + " resouces leaked.")
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
  def report(trace:Any) = {
    Tracing.tracing.foreach { tr =>
      tr.open.get(trace).foreach { e =>
        l.error( trace + " was opened here ", e)
      }
      tr._closed.get(trace).foreach { e =>
        l.error( trace + " was created here", e._1)
        l.error( "then " + trace + " was deleted here", e._2)
      }
    }
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
