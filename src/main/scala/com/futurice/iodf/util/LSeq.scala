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

  /**
   * TODO: rename this, lazy implies cacheing, while we don't cache anything
   */
  def lazyMap[B](f: T => B) : LSeq[B] = map[B](f)
  def map[B](f: T => B) : LSeq[B] = {
    val self = this
    new LSeq[B] {
      def apply(l:Long) = f(self.apply(l))
      def lsize : Long = self.lsize
      override def iterator : Iterator[B] = self.iterator.map(f)
      override def close = self.close
    }
  }
  def zipWithIndex : LSeq[(T, Long)] = {
    val self = this
    new LSeq[(T, Long)] {
      override def apply(l: Long) = (self(l), l)
      override def lsize = self.lsize
      override def iterator : Iterator[(T, Long)] = new Iterator[(T, Long)] {
        val i = self.iterator
        var at = 0L
        override def hasNext = i.hasNext
        override def next() = {
          val rv = (i.next(), at)
          at += 1
          rv
        }
      }
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
  def select(indexes:LSeq[Long]) = {
    val self = this
    indexes.map { i : Long => self(i) }
  }
  def selectSome(indexes:LSeq[Option[Long]]) = {
    val self = this
    indexes.map { e : Option[Long] => e.map { index =>
        self(index)
      }
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
    override def iterator = v.iterator
  }
  def from[T](v:Array[T]) = new LSeq[T] {
    override def apply(l: Long): T = v(l.toInt)
    override def lsize: Long = v.size
    override def iterator = v.iterator
  }
}

