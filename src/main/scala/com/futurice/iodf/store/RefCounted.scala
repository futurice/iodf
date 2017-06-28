package com.futurice.iodf.store

import java.io.Closeable
import java.util.logging.{Level, Logger}

import com.futurice.iodf.IoScope

import scala.collection.mutable

object RefCounted {
  val l = Logger.getLogger("RefCounted")
  @volatile
  var openRefs = 0
  var traces : Option[mutable.Map[RefCounted[_], Exception]] = None


  def opened(ref:RefCounted[_ <: java.io.Closeable]) = synchronized {
    traces match {
      case Some(tr) => tr += (ref -> new RuntimeException("leaked reference"))
      case None =>
    }
    openRefs += 1
  }

  def closed(ref:RefCounted[_]) = synchronized {
    traces match {
      case Some(tr) =>
        tr.remove(ref)
      case None =>
    }
    openRefs -= 1
  }

  def trace[T](f : => T) : T = {
    // FIXME: not thread safe! Use threadlocal!
    val tracesBefore = traces
    traces = Some(new mutable.HashMap[RefCounted[_], Exception]())
    try {
      f
    } finally {
      if (traces.get.size > 0) {
        l.log(Level.SEVERE, traces.get.size + " references leaked.")
        traces.get.take(1).map { t =>
          l.log(Level.SEVERE, t._1.value + " leaked with count " + t._1.count + "!", t._2)
        }
      }
      traces = tracesBefore
    }
  }
}

case class RefCounted[V <: Closeable](val value:V, val initCount:Int) extends Closeable {
  @volatile var count = initCount
  RefCounted.opened(this)

  // FIXME: the API needs redesign based on real-world usage patterns

  // NEW VERSION of apply() !
  //  def apply(implicit scope:IoScope) = inc.bind.get // this would be consistent with other methods

  //  def apply() = value
  def get = value

  /*  def bind(implicit scope:IoScope) = {
      inc
      scope.bind(this)
      this
    }*/
  def inc = synchronized  {
    count += 1
    this
  }
  def bind(implicit scope:IoScope) = {
    scope.bind(this)
  }
  // prefer .inc.bind.get
  def incBindAndGet(implicit scope:IoScope) = inc.bind.get
  def bindAndGet(implicit scope:IoScope) = bind.get
  def incAndBind(implicit scope:IoScope) = inc.bind
  def incAndGet = inc.get

  def closing [RV](f : (V) => RV) : RV = {
    try { f(value) }
    finally { close }
  }
  //  def use(implicit scope:IoScope) = incBindAndGet(scope)
  // incAndGet
  def open = incAndGet
  override def hashCode(): Int = value.hashCode()
  def close = synchronized {
    //    System.out.println("dec " + RefCounted.this.hashCode())
    //    new RuntimeException().printStackTrace()
    if (count <= 0) throw new RuntimeException("cannot decrease count as it is already " + count)
    count -= 1
    if (count == 0) {
      //      System.out.println("closed " + RefCounted.this.hashCode())
      //      new RuntimeException().printStackTrace()
      RefCounted.closed(this)
      value.close
    }
  }
}

