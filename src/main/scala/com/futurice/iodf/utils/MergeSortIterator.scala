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

class MultiIterator[T](i:Array[Iterator[T]]) extends Iterator[T] {

  val is = new PeekIterator[Iterator[T]](i.iterator)

  def prepareNext: Unit = {
    while (is.hasNext && !is.head.hasNext) is.next
  }

  prepareNext

  override def hasNext: Boolean = {
    is.hasNext && is.head.hasNext
  }

  override def next(): T = {
    val rv = is.head.next()
    prepareNext
    rv
  }
}

class MultiIterable[T](i:Array[Iterable[T]]) extends Iterable[T] {
  override def iterator: Iterator[T] = {
    new MultiIterator[T](i.map(_.iterator))
  }
}


case class MergeSortEntry[T](sources:Array[Int], sourceIndexes:Array[Long], index:Long, value:T)

/**
  * Created by arau on 6.6.2017.
  */
class MergeSortIterator[T](is:Seq[Iterator[T]])(implicit ord:Ordering[T]) extends Iterator[MergeSortEntry[T]] {

  type Entry = MergeSortEntry[T]

  private val peeked = is.map(i => new PeekIterator[T](i)).toArray
  private val indexes = Array.fill(peeked.size)(0L)
  private var index = 0L

  def getNext : Option[Entry] = synchronized {
    peeked.zipWithIndex
          .flatMap { case (it, index) => it.headOption.map { (_, index) } }
          .sorted match {
      case entries if entries.size > 0 =>
        val (value, iterator) = entries.head
        val sources = entries.filter(_._1 == value).map(_._2)
        val sourceIndexes =
          sources.map { source =>
            peeked(source).next // move forward
            val rv = indexes(source)
            indexes(source) = rv + 1
            rv
          }
        val i = index
        index += 1
        Some(MergeSortEntry[T](sources, sourceIndexes, i, value))
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
