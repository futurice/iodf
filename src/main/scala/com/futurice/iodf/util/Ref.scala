package com.futurice.iodf.util

import java.io.{Closeable, PrintWriter, StringWriter}

import com.futurice.iodf.IoScope

/**
  * Created by arau on 4.7.2017.
  */

/**
  * Handles are used for lifecyles & resource management.
  * You can copy a handle, or you can close a handle
  */
trait Handle extends Closeable {
  def openCopy : Handle
}

trait Ref[T] extends Handle{
  def isClosed : Boolean
  def get : T
  def openCopy : Ref[T]
  def openMap[E](f:T => E) : Ref[E] = {
    val c : Ref[T] = openCopy
    new Ref[E] {
      override def get: E = f(c.get)
      override def openCopy: Ref[E] = c.openMap(f)
      override def close(): Unit = c.close
      override def isClosed = c.isClosed
    }
  }

  // helper
  def copy(implicit bind:IoScope): Ref[T] = {
    bind(openCopy)
  }
  def map[E](f: T => E )(implicit bind:IoScope) : Ref[E] = {
    bind(openMap(f))
  }
}

case class RefCount(closer:() => Unit, var v:Int = 0) {
  var isClosed = false
//  Tracing.opened(this)
  def inc = synchronized { v += 1 }
  def dec = synchronized {
    v -= 1
    if (v == 0) {
      if (isClosed) throw new RuntimeException("double closing!")
      closer()
//      Tracing.closed(this)
      isClosed = true
    }
  }
  override def toString = f"RefCount($closer, $v)"
}

object Ref {

  def open[T](value:T, refCount:RefCount) : Ref[T] = new Ref[T] {
    var _isClosed = false

    refCount.inc
    Tracing.opened(this)
    override def get: T = {
      if (refCount.isClosed) throw new RuntimeException("closed " + value + " accessed")
      value
    }
    override def openCopy: Ref[T] = Ref.open[T](value, refCount)
    override def isClosed = _isClosed || refCount.isClosed
    override def close(): Unit = {
      refCount.dec
      _isClosed = true
      Tracing.closed(this)
    }
    override def toString =
      value.toString + "@" + refCount
  }

  def open[T](value:T, closer:() => Unit ): Ref[T] = {
    open[T](value, new RefCount(closer, 0))
  }

  def open[T <: Closeable](value:T): Ref[T] = {
    open[T](value, () => value.close)
  }

}
