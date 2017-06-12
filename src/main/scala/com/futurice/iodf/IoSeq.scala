package com.futurice.iodf

import java.io.{Closeable, DataOutputStream, InputStream, OutputStream}

import com.futurice.iodf.store.RandomAccess


/* long sequence interface */
trait LSeq[T] extends Iterable[T] with PartialFunction[Long, T] with Closeable {
  def close = {}
  def apply(l:Long) : T
  def lsize : Long
  override def size = lsize.toInt
  def isDefinedAt(l:Long) = l >= 0 && l < size
  override def iterator : Iterator[T] = {
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
  def map[B](f: T => B) : LSeq[B] = {
    val self = this
    new LSeq[B] {
      def apply(l:Long) = f(self.apply(l))
      def lsize : Long = self.lsize
    }
  }
}

object LSeq {
  def fill[T](n:Long, t:T) = new LSeq[T] {
    override def apply(l: Long): T = t
    override def lsize: Long = n
  }
  def apply[T](v:Seq[T]) = new LSeq[T] {
    override def apply(l: Long): T = v(l.toInt)
    override def lsize: Long = v.size
  }
  def apply[T](v:Array[T]) = new LSeq[T] {
    override def apply(l: Long): T = v(l.toInt)
    override def lsize: Long = v.size
  }
}

class MultiSeq[T, S <: LSeq[T]](seqs:Array[S]) extends LSeq[T] with Closeable {

  val scope = new IoScope()
  seqs.foreach {
    _ match {
      case c: Closeable => scope.bind(c)
      case _ =>
    }
  }

  override def close(): Unit = {
    scope.close()
  }

  val (ranges, lsize) = {
    var offset = 0L
    (seqs.map { seq =>
      val begin = offset
      offset += seq.lsize
      (begin, offset)
    }, offset)
  }
  def toSeqIndex(index:Long) = {
    ranges.zipWithIndex.find( index < _._1._2 ).map { case ((begin, end), seqIndex) =>
      (seqs(seqIndex), index - begin)
    }
  }

  // Potentially slow, because O(N) complexity
  override def apply(l: Long): T = {
    val Some((s, sIndex)) = toSeqIndex(l)
    s(sIndex)
  }
}

trait IoIterable[Id, T] extends IoObject[Id] with Iterable[T] {
  def iterator : Iterator[T]
}


trait IoSeqType[IoId, Member, Interface <: LSeq[Member], Distributed <: IoSeq[IoId, Member] with Interface] extends IoType[IoId, Distributed] with WithValueTypeTag[Member] {
  def defaultSeq(lsize:Long) : Option[Interface] = None

  def viewMerged(seqs:Seq[Interface]) : Interface
  def viewAnyMerged(seqs:Seq[Any]) : Interface =
    viewMerged(seqs.map(_.asInstanceOf[Interface]))

  def writeSeq(out:DataOutputStream, v:Interface) : Unit
  def writeAnySeq(out:DataOutputStream, v:LSeq[_]) = {
    v match {
      case i:Interface => writeSeq(out, i)
    }
  }
  def writeMerged(out:DataOutputStream, ss:Seq[Interface]) = {
    writeSeq(out, viewMerged(ss))
  }
  def writeAnyMerged(out:DataOutputStream, ss:Seq[Any]) = {
    writeMerged(out, ss.map(_.asInstanceOf[Interface]))
  }
}

trait IoSeq[Id, T] extends IoIterable[Id, T] with LSeq[T] {}
