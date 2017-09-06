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

trait Ref[+T] extends Handle{
  def isClosed : Boolean
  def get : T
  def openCopy : Ref[T]
  def openCopyMap[E](f:T => E) : Ref[E] = {
    val c : Ref[T] = openCopy
    new Ref[E] {
      override def get: E = f(c.get)
      override def openCopy: Ref[E] = c.openCopyMap(f)
      override def close(): Unit = c.close
      override def isClosed = c.isClosed
    }
  }
  def openCopyAs[E] = openCopyMap(_.asInstanceOf[E])
  def map[E](f:T => E) : Ref[E] = {
    val c = this
    new Ref[E] {
      override def get: E = f(c.get)
      override def openCopy: Ref[E] = c.openCopyMap(f)
      override def close(): Unit = c.close
      override def isClosed = c.isClosed
    }
  }
  def as[E] = map(_.asInstanceOf[E])

  // helper
  def copy(implicit bind:IoScope): Ref[T] = {
    bind(openCopy)
  }
  def copyMap[E](f: T => E )(implicit bind:IoScope) : Ref[E] = {
    bind(openCopyMap(f))
  }
  def copyAs[E](implicit bind:IoScope) = copyMap(_.asInstanceOf[E])

}

class RefCount(trace:Any , closer:() => Unit, var v:Int = 0) {
  var isClosed = false
  Tracing.opened(this)
  def inc = synchronized {
    if (isClosed) {
      Tracing.report(this)
      throw new RuntimeException("incrementing already closed refcount " + this + "!")
    }
    v += 1
  }
  def dec = synchronized {
    v -= 1
    if (v == 0) {
      Tracing.closed(this)
//      if (isClosed) throw new RuntimeException("double closing of " + trace + "!")
      closer()
      isClosed = true
    }
  }
  override def toString = f"RefCount($trace, $v)"
}

object Ref {

  def unapply[T](value:Ref[T]) : Option[T] = {
    if (value.get.isInstanceOf[T]) {
      Some(value.get.asInstanceOf[T])
    } else {
      None
    }
  }

  def open[T](value:T, refCount:RefCount) : Ref[T] = new Ref[T] {
    var _isClosed = false
//    var _opened = new RuntimeException("opened here")

    refCount.inc
    Tracing.opened(this)
    override def get: T = {
      if (refCount.isClosed) {
        Tracing.report(refCount)
        throw new RuntimeException("closed " + value + " accessed")
      }
      value
    }
    override def openCopy: Ref[T] = Ref.open[T](value, refCount)
    override def isClosed = _isClosed || refCount.isClosed
    override def close(): Unit = {
      Tracing.closed(this)
      refCount.dec
      _isClosed = true
    }
    override def toString =
      value.toString + "@" + refCount
    /*   override def toString() : String = {
      val str = new StringWriter()
      val buf = new PrintWriter(str)
      buf.append(value.toString + "@" + refCount)
      _opened.printStackTrace(buf)
      buf.flush()
      str.toString
    }*/
  }

  def mock[T](value:T) : Ref[T] = new Ref[T] {
    var isClosed = false;

    override def get: T = value

    override def openCopy: Ref[T] = mock[T](value)

    override def close(): Unit = isClosed = true
  }

  def open[T](value:T, closer:() => Unit ): Ref[T] = {
    open[T](value, new RefCount(value, closer, 0))
  }

  def open[T](value:T): Ref[T] = {
    value match {
      case c: Closeable =>
        open[T](value, () => c.close)
      case v =>
        open[T](value, () => {})
    }
  }

  def apply[T](value:T)(implicit bind:IoScope)  = {
    bind(open[T](value))
  }

}
