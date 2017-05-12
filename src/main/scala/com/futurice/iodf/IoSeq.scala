package com.futurice.iodf

import scala.collection.generic.CanBuildFrom


trait IoIterable[Id, T] extends IoObject[Id] with Iterable[T] {
  def iterator : Iterator[T]
}

trait IoSeq[Id, T] extends IoIterable[Id, T] with PartialFunction[Long, T] {
  // Potentially slow, because O(N) complexity
  def apply(l:Long) : T
  def lsize : Long
  override def size = lsize.toInt
  def isDefinedAt(l:Long) = l >= 0 && l < size
  override def iterator = {
    new Iterator[T] {
      var i = 0L
      override def hasNext: Boolean = {
        i < lsize
      }
      override def next(): T = {
        val rv = apply(i)
        i += 1
        rv
      }
    }
  }

/*
  def map[B, That](f: A => B)(implicit bf: CanBuildFrom[Repr, B, That]): That = {
    def builder = { // extracted to keep method size under 35 bytes, so that it can be JIT-inlined
      val b = bf(repr)
      b.sizeHint(this)
      b
    }
    val b = builder
    for (x <- this) b += f(x)
    b.result
  }


 */

  /** WARNING: closing the mapped ioseq will close this ioseq! */
//  , That](f: A => B)(implicit bf: CanBuildFrom[Repr, B, That]): That =
  def map[B](f: T => B) : IoSeq[Id, B] = {
    val self = this
    new IoSeq[Id, B] {
      def ref = throw new NotImplementedError()
      def apply(l:Long) = f(self.apply(l))
      def lsize : Long = self.lsize
      def close = {
        self.close
      }
    }
  }
}
