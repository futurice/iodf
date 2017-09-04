package com.futurice.iodf.util

import java.io.Closeable

import com.futurice.iodf.IoScope

class MultiSeq[T, S <: LSeq[T]](_refs:Array[Ref[_ <: S]]) extends LSeq[T] with Closeable {

  val scope = new IoScope()

  Tracing.opened(this)

  val refs = _refs.map(_.copy(scope))
  val seqs = refs.map(_.get)

  override def close(): Unit = {
    scope.close()
    Tracing.closed(this)
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

