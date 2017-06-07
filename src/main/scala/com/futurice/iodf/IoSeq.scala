package com.futurice.iodf

import java.io.DataOutputStream

import scala.collection.generic.CanBuildFrom

class MultiSeq[IoId, T, S <: IoSeq[_, T]](seqs:Array[S]) extends IoSeq[IoId, T] {

  val scope = new IoScope()
  seqs.foreach(scope.bind)
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

  override def ref: IoRef[IoId, _ <: IoObject[IoId]] = {
    throw new RuntimeException("multidf colId is not referable")
  }
}

trait IoIterable[Id, T] extends IoObject[Id] with Iterable[T] {
  def iterator : Iterator[T]
}

trait SeqIoType[Id, T <: IoObject[Id], M] extends IoType[Id, T] with WithValueTypeTag[M] {
  def defaultSeq(lsize:Long) : IoSeq[Id, M] = {
    throw new RuntimeException("not implemented")
  }
  def viewMerged(seqs:Array[IoSeq[Id, M]]) : IoSeq[Id, M] = new MultiSeq[Id, M, IoSeq[Id, M]] (seqs)
  def viewAnyMerged(seqs:Array[IoSeq[Id, _]]) : IoSeq[Id, M] =
    viewMerged(seqs.map(_.asInstanceOf[IoSeq[Id, M]]))
  def writeMerged(out:DataOutputStream, seqA:T, seqB:T) : Unit
  def writeAnyMerged(out:DataOutputStream, seqA:Any, seqB:Any) = {
    writeMerged(out, seqA.asInstanceOf[T], seqB.asInstanceOf[T])
  }
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
