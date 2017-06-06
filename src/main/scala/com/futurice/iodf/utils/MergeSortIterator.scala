package com.futurice.iodf.utils

class PeekIterator[T](i:Iterator[T]) extends Iterator[T] {

  private def peekNext = {
    i.hasNext match {
      case true => Some(i.next)
      case false => None
    }
  }
  private var peek : Option[T] = peekNext

  def headOption = peek
  def head = peek.get

  def hasNext = {
    peek.isDefined
  }

  def next = {
    val rv = peek.get
    peek = peekNext
    rv
  }
}

case class MergeSortEntry[T](source:Int, indexInSource:Long, index:Long, value:T)

/**
  * Created by arau on 6.6.2017.
  */
class MergeSortIterator[T](is:Seq[Iterator[T]])(implicit ord:Ordering[T]) extends Iterator[MergeSortEntry[T]] {

  type Entry = MergeSortEntry[T]

  private val peeked = is.map(i => new PeekIterator[T](i)).toArray
  private val indexes = Array.fill(peeked.size)(0L)
  private var index = 0L

  def getNext : Option[Entry] = {
    peeked.zipWithIndex
          .flatMap { case (it, index) => it.headOption.map { (_, index) } } match {
      case entries if entries.size > 0 =>
        val (value, iterator) = entries.min
        peeked(iterator).next // move forward
        val indexInSource = indexes(iterator)
        val i = index
        indexes(iterator) += 1
        index += 1
        Some(MergeSortEntry[T](iterator, indexInSource, i, value))
      case _ =>
        None
    }
  }

  private var n = getNext

  override def hasNext: Boolean = n.isDefined

  def headOption = n
  def head = headOption.get

  def next() = {
    val rv = head
    n = getNext
    rv
  }
}
