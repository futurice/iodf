package com.futurice.iodf

import com.futurice.iodf.store.RefCounted
import com.futurice.iodf.utils.MergeSortIterator

import scala.collection.mutable.ArrayBuffer


class MultiCol[IoId, ColId] extends IoSeq[IoId, ColId] {

}

/**
  * Created by arau on 6.6.2017.
  */
class MultiDf[IoId, ColId](dfs:Array[RefCounted[Df[IoId, ColId]]])(
  implicit val colIdOrdering:Ordering[ColId]) extends Df[IoId, ColId] {

  val scope = new IoScope()
  dfs.foreach { _.bind(scope) }
  override def close(): Unit = {
    scope.close
  }

  override val colIds: IoSeq[IoId, ColId] = {
    new IoSeq[IoId, ColId] {
      val seqs : Array[IoSeq[IoId, ColId]] = dfs.map(_.value.colIds).toArray

      val mergeIterator =
        new MergeSortIterator[ColId](seqs.map(_.iterator))

      val values = new ArrayBuffer[ColId]()

      def updateTo(index:Long) = {
        while (mergeIterator.headOption.map(_.index < index).getOrElse(false)) {
          values += mergeIterator.next().value
        }
      }

      override def apply(l: Long): ColId = {
        updateTo(l)
        values(l.toInt)
      }

      override def lsize: Long = {
        updateTo(Long.MaxValue)
        values.size
      }

      override def ref: IoRef[IoId, _ <: IoObject[IoId]] = {
        //
      }
      override def close(): Unit = {
        //
      }
    }
  }

  def concatOpenedColumns(cols:Array[IoSeq[IoId, Any]]) =
    new IoSeq[IoId, ColId] {
      implicit val scope = new IoScope()
      val seqs :  = dfs.map(_.value.colIds).toArray
      val (offsets, lsize) = {
        var offset = 0L
        (seqs.map { seq =>
          val rv = offset
          offset += seq.lsize
          offset
        }, offset)
      }
      def toSeqIndex(index:Long) = {
        offsets.zipWithIndex.find( index >= _._1 && index < lsize).map { case (offset, seqIndex) =>
          (seqs(seqIndex), index - offset)
        }
      }

      // Potentially slow, because O(N) complexity
      override def apply(l: Long): ColId = {
        val Some((s, offset)) = toSeqIndex(l)
        s(offset)
      }

      override def ref: IoRef[IoId, _ <: IoObject[IoId]] = {
        throw new RuntimeException("multidf colId is not referable")
      }

      override def close(): Unit = {
        //  nothing
      }
    }


  override val _cols: IoSeq[IoId, IoSeq[IoId, Any]] =
    new IoSeq[IoId, IoSeq[IoId, Any]]{
      // Potentially slow, because O(N) complexity
      override def apply(l: Long): IoSeq[IoId, Any] = {

      }
      override def lsize: Long = {

      }
      override def ref: IoRef[IoId, _ <: IoObject[IoId]] = {

      }

      override def close(): Unit = {

      }
    }

  // size in Long
  override lazy val lsize: Long = dfs.map(_.value.lsize).sum

}

