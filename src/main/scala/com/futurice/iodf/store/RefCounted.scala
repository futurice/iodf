package com.futurice.iodf.store

import java.io.Closeable
import java.util.logging.{Level, Logger}

import com.futurice.iodf.IoScope
/*
case class RefCounted[V <: Closeable](val value:V, val initCount:Int) extends Ref[V] {
  @volatile var count = initCount
  RefCounted.opened(this)

  def copy = inc // 'copies' the reference by incrementing & returning self
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
}*/

