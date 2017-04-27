package com.futurice.iodf


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
}
