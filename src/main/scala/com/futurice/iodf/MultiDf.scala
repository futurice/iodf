package com.futurice.iodf

import com.futurice.iodf.store.RefCounted
import com.futurice.iodf.utils.{MergeSortEntry, MergeSortIterator}

import scala.collection.mutable.ArrayBuffer

object MultiDf {
  def apply[IoId, ColId](dfs:Seq[_ <: Df[IoId, ColId]], types:DfColTypes[IoId, ColId])(
    implicit colIdOrdering:Ordering[ColId]) = {
    new MultiDf[IoId, ColId](dfs.toArray, types)
  }
}


/**
  * Created by arau on 6.6.2017.
  */
class MultiDf[IoId, ColId](dfs:Array[_ <: Df[IoId, ColId]], types:DfColTypes[IoId, ColId])(
  implicit val colIdOrdering:Ordering[ColId]) extends Df[IoId, ColId] {

 /* val scope = new IoScope()
  dfs.foreach { df => scope.bind(df) }*/
  override def close(): Unit = {
//    scope.close
  }

  val seqs : Array[IoSeq[IoId, ColId]] = dfs.map(_.colIds).toArray
  val mergeIterator =
    new MergeSortIterator[ColId](seqs.map(_.iterator))
  val entries = new ArrayBuffer[MergeSortEntry[ColId]]()

  def updateTo(index:Long) = {
    while (mergeIterator.headOption.map(_.index < index).getOrElse(false)) {
      entries += mergeIterator.next()
    }
  }

  override val colIds: IoSeq[IoId, ColId] = {
    new IoSeq[IoId, ColId] {

      override def apply(l: Long): ColId = MultiDf.this.synchronized {
        updateTo(l)
        entries(l.toInt).value
      }

      override def lsize: Long = MultiDf.this.synchronized {
        updateTo(Long.MaxValue)
        entries.size
      }

      override def ref: IoRef[IoId, _ <: IoObject[IoId]] = {
        throw new RuntimeException("not referable")
      }
      override def close(): Unit = {
        //
      }
    }
  }

  override val _cols: IoSeq[IoId, IoSeq[IoId, Any]] =
    new IoSeq[IoId, IoSeq[IoId, Any]] {
      // this will open the column
      override def apply(l: Long): IoSeq[IoId, Any] = MultiDf.this.synchronized {
        updateTo(l)
        val e = entries(l.toInt)
        val cols = e.sources zip e.sourceIndexes map { case (source, index) =>
          dfs(source).openCol(index)
        }
        val t = types.colType(e.value)
        val colMap = (e.sources zip cols).toMap
        t.viewAnyMerged(
          (0 until dfs.size).map {
            i => colMap.getOrElse(i, t.defaultSeq(dfs(i).lsize)).asInstanceOf[IoSeq[IoId, Any]]
          }).asInstanceOf[IoSeq[IoId, Any]]
      }
      override def lsize: Long = {
        MultiDf.this.lsize
      }
      override def ref: IoRef[IoId, _ <: IoObject[IoId]] = {
        throw new RuntimeException("not referable")
      }
      override def close(): Unit = {
        // nothing
      }
    }

  // size in Long
  override lazy val lsize: Long = dfs.map(_.lsize).sum

}

