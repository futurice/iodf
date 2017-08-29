package com.futurice.iodf.util

import com.futurice.iodf.Utils

trait SeekIterator[+T, S] extends Iterator[T] {
  /* seeks the position after the given position */
  def seek(t:S) : Boolean
  def seeked(t:S) = {
    seek(t)
    this
  }
}

trait PeekIterator[T] extends Iterator[T] {
  def head : T
  def headOption : Option[T] =
    hasNext match {
      case true => Some(head)
      case false => None
    }
  def scan(t:T)(implicit ord:Ordering[T]) : Boolean = {
    while (hasNext && ord.lt(head, t)) next
    head == t
  }
  def scanUntil(until:T => Boolean)(implicit ord:Ordering[T]) : Boolean = {
    while (hasNext && !until(head)) next
    hasNext && until(head)
  }
  def scanned(t:T)(implicit ord:Ordering[T]) : PeekIterator[T] = {
    scan(t)
    this
  }
}

trait PeekIterable[T] extends Iterable[T] {
  def iterator : PeekIterator[T]
}
trait Scanner[T, S] extends SeekIterator[T, S] with PeekIterator[T] {
  def copy : Scanner[T, S]
}
object Scanner {
  def apply[T](orderedSeq:LSeq[T], from:Long = 0L)(implicit ord:Ordering[T]) : Scanner[T, T]= {
    new Scanner[T, T] {
      var i = from
      override def copy = Scanner.apply(orderedSeq, i)
      override def seek(t:T) = {
        val (at, low, high) =
          Utils.binarySearch[T](orderedSeq, t, i, orderedSeq.lsize)
        i = high
        at != -1
      }
      override def head : T = orderedSeq(i)
      override def hasNext: Boolean = {
        i < orderedSeq.lsize
      }
      override def next(): T = {
        val rv = orderedSeq(i)
        i += 1
        rv
      }
    }
  }
}

trait Scannable[T,S] extends PeekIterable[T] {
  def iterator : Scanner[T, S]
}

object PeekIterator {
  def apply[T](i:Iterator[T]) = new PeekIterator[T] {
    private def peekNext = {
      i.hasNext match {
        case true => Some(i.next)
        case false => None
      }
    }
    private var peek : Option[T] = peekNext

    override def headOption = peek
    def head = {
      peek match {
        case Some(v) => v
        case None =>
          throw new NoSuchElementException("PeekIterator.head @ " + i.toString())
      }
    }
    def hasNext = {
      peek.isDefined
    }

    def next = {
      val rv = peek.get
      peek = peekNext
      rv
    }
  }


}

class MultiIterator[T](i:Array[Iterator[T]]) extends Iterator[T] {

  val is = PeekIterator(i.iterator)

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


case class MergeSortEntry[T](sources:Array[Int], allSourceIndexes:Array[Long], index:Long, value:T) {
  def sourceIndexes = sources.map(allSourceIndexes)
  override def toString = f"((${sources.mkString(",")}), (${sourceIndexes.mkString(",")}), $index, $value)"
}

object MergeSortIterator {
  def apply[T](is:Array[_ <: Iterator[T]])(implicit ord:Ordering[T]) : MergeSortIterator[T] = {
    apply(is.map(i => PeekIterator(i)))
  }
  def apply[T](is:Array[PeekIterator[T]])(implicit ord:Ordering[T]) : MergeSortIterator[T] = {
    new MergeSortIterator[T](is, Array.fill(is.size)(0L), 0L)
  }
  def seek[T](is:Array[_ <: Scanner[T, T]], pos: MergeSortEntry[T])(implicit ord:Ordering[T]) = {
    is.foreach { is =>
      is.seek(pos.value)
    }
    fromReady(is, pos)
  }
  def scan[T](is:Array[_ <: PeekIterator[T]], pos: MergeSortEntry[T])(implicit ord:Ordering[T]) = {
    is.foreach { is =>
      is.scan(pos.value)
    }
    fromReady(is, pos)
  }
  def fromReady[T](is:Array[_ <: PeekIterator[T]], pos: MergeSortEntry[T])(implicit ord:Ordering[T]) = {
    new MergeSortIterator[T](is, pos.allSourceIndexes.clone, pos.index)
  }
}

/**
  * Created by arau on 6.6.2017.
  */
class MergeSortIterator[T](peeked:Array[_ <: PeekIterator[T]], val sourceIndexes : Array[Long], var index:Long)(implicit ord:Ordering[T])
  extends PeekIterator[MergeSortEntry[T]] {

  type Entry = MergeSortEntry[T]

  def getNext : Option[Entry] = synchronized {
    peeked.zipWithIndex
          .flatMap { case (it, index) => it.headOption.map { (_, index) } }
          .sorted match {
      case entries if entries.size > 0 =>
        val (value, iterator) = entries.head
        val sources = entries.filter(_._1 == value).map(_._2)
        val allSourceIndexes = sourceIndexes.clone()
        sources.foreach { source =>
          peeked(source).next // move forward
          sourceIndexes(source) = sourceIndexes(source) + 1
        }
        val i = index
        index += 1
        Some(MergeSortEntry[T](sources, allSourceIndexes, i, value))
      case _ =>
        None
    }
  }

  private var n = getNext

  override def hasNext: Boolean = n.isDefined

  def headIndexes = {
    val rv = sourceIndexes.clone
    (head.sources zip head.sourceIndexes) foreach { case (s, i) =>
      rv(s) = i
    }
    rv
  }

  override def headOption = n
  def head = headOption.get

  def next() = {
    val rv = head
    n = getNext
    rv
  }

  def scanValue(value:T) = {
    while (hasNext && ord.lt(head.value, value)) next
    head.value == value
  }
  def scannedValue(value:T) = {
    scanValue(value)
    this
  }

  def scanIndex(index: Long) = {
    while (hasNext && head.index < index) next
    hasNext
  }
  def scannedIndex(index: Long) = {
    scanIndex(index)
    this
  }


}
