package com.futurice.iodf.util

import java.io.Closeable

import com.futurice.iodf.IoScope

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

