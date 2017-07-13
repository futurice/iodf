package com.futurice.iodf.util

import java.io.Closeable


/* long sequence interface */
trait LSeq[+T] extends Iterable[T] with PartialFunction[Long, T] with Closeable {
  def close = {}
  def apply(l:Long) : T
  def lsize : Long
  override def size = lsize.toInt
  def isDefinedAt(l:Long) = l >= 0 && l < size
  def view(from:Long, until:Long) : LSeq[T] = {
    val self = this
    new LSeq[T] {
      def lsize = until - from
      def apply(l:Long) = self.apply(l+from)
    }
  }
  def map[B](f: T => B) : LSeq[B] = {
    val self = this
    new LSeq[B] {
      def apply(l:Long) = f(self.apply(l))
      def lsize : Long = self.lsize
      override def close = self.close
    }
  }
  def iterator : Iterator[T] = new Iterator[T] {
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
  override def toString =
    f"LSeq(" + (if (lsize < 8) this.mkString(", ") else this.take(8).mkString(", ") + "..") + ")"
}

object LSeq {
  def fill[T](n:Long, t:T) = new LSeq[T] {
      override def apply(l: Long): T = t
      override def lsize: Long = n
  }
  def empty[T] = new LSeq[T] {
    override def apply(l:Long) = throw new IndexOutOfBoundsException(f"this sequence is empty")
    override def lsize = 0
  }
  def apply[T](v:T*) = from[T](v)
  def from[T](v:Seq[T]) = new LSeq[T] {
    override def apply(l: Long): T = v(l.toInt)
    override def lsize: Long = v.size
    override def iterator = PeekIterator(v.iterator)
  }
  def from[T](v:Array[T]) = new LSeq[T] {
    override def apply(l: Long): T = v(l.toInt)
    override def lsize: Long = v.size
    override def iterator = PeekIterator(v.iterator)
  }
}

