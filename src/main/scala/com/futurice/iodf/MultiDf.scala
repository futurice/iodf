package com.futurice.iodf

import com.futurice.iodf.store.RefCounted
import com.futurice.iodf.utils.{MergeSortEntry, MergeSortIterator, PeekIterator, Scanner}

import scala.collection.mutable.ArrayBuffer

object MultiDf {
  def DefaultColIdMemRatio = 16
  def apply[IoId, ColId](dfs:Seq[_ <: Df[IoId, ColId]], types:DfColTypes[IoId, ColId])(
    implicit colIdOrdering:Ordering[ColId]) = {
    new MultiDf[IoId, ColId](dfs.toArray, types)
  }
  def refCounted[IoId, ColId](dfs:Seq[_ <: RefCounted[Df[IoId, ColId]]], types:DfColTypes[IoId, ColId])(
    implicit colIdOrdering:Ordering[ColId]) = {
    val rv = new MultiDf[IoId, ColId](dfs.map(_.value).toArray, types)
    dfs.map { rv.scope.bind(_) }
    rv
  }
  def autoClosing[IoId, ColId](dfs:Seq[_ <: Df[IoId, ColId]],
                               types:DfColTypes[IoId, ColId], colIdMemRatio:Int = DefaultColIdMemRatio)(
    implicit colIdOrdering:Ordering[ColId]) = {
    val rv = new MultiDf[IoId, ColId](dfs.toArray, types, colIdMemRatio)
    dfs.map { rv.scope.bind(_) }
    rv
  }
}


/**
  * Created by arau on 6.6.2017.
  */
class MultiDf[IoId, ColId](dfs:Array[_ <: Df[IoId, ColId]], types:DfColTypes[IoId, ColId], val colIdMemRatio: Int = 32)(
  implicit val colIdOrdering:Ordering[ColId]) extends Df[IoId, ColId] {

  type ColType[T] = MultiSeq[T, LSeq[T]]

  val scope = new IoScope()
  /* dfs.foreach { df => scope.bind(df) }*/
  override def close(): Unit = {
    scope.close
  }
  val seqs : Array[LSeq[ColId]] = dfs.map(_.colIds)

  val jumpEntries = new ArrayBuffer[MergeSortEntry[ColId]]

  private val colIdsLsize = {
    var at = 0L
    val i = MergeSortIterator[ColId](seqs.map(_.iterator))
    while (i.hasNext) {
      if (at % colIdMemRatio == 0) {
        jumpEntries += i.head
      }
      at += 1
      i.next
    }
    at
  }

  def jumpIterator(jumpEntry:MergeSortEntry[ColId]) = {
    MergeSortIterator.fromReady(
      (seqs zip jumpEntry.allSourceIndexes).map { case (seq, jump) =>
        Scanner(seq, jump) : PeekIterator[ColId]
      }, jumpEntry)
  }

  def entryOfIndex(index:Long) : MergeSortEntry[ColId] = {
    val jumpIndex = (index / colIdMemRatio).toInt
    val jumpEntry = jumpEntries(jumpIndex)

    if (index == jumpIndex) {
      jumpEntry
    } else {
      jumpIterator(jumpEntry).scannedIndex(index).head
    }
  }

  def entryOfId(id: ColId) : MergeSortEntry[ColId] = {
    val jumpIndex =
      Utils.binarySearch(LSeq(jumpEntries).map[ColId](_.value), id)(colIdOrdering)._2.toInt
    val jumpEntry = jumpEntries(jumpIndex)

    if (id == jumpEntry.value) {
      jumpEntry
    } else {
      jumpIterator(jumpEntry).scannedValue(id).head
    }
  }


  override def indexOf(id: ColId): Long = {
    val e = entryOfId(id)
    if (e.value == id) {
      e.index
    } else {
      -1
    }
  }

  /*  val colIdEntries = colIdIterator.toArray
  def colIdEntry(i:Long) = colIdEntries(i.toInt)*/

  override val colIds: LSeq[ColId] = {
    new LSeq[ColId] {
      override def apply(l: Long): ColId = MultiDf.this.synchronized {
        entryOfIndex(l).value
      }
      override def lsize: Long = colIdsLsize
      override def iterator =
        PeekIterator(
          if (colIdMemRatio == 1) { // special case optimization
            jumpEntries.map(_.value).iterator
          } else {
            MergeSortIterator[ColId](seqs.map(_.iterator)).map(_.value)
        })
    }
  }

  override val _cols: LSeq[ColType[Any]] =
    new LSeq[ColType[Any]] {
      // this will open the column
      override def apply(l: Long): ColType[Any] = MultiDf.this.synchronized {
        val e = entryOfIndex(l)
        val cols = e.sources zip e.sourceIndexes map { case (source, index) =>
          dfs(source).openCol(index)
        }
        val t = types.colType(e.value)
        val colMap = (e.sources zip cols).toMap
        t.viewAnyMerged(
          (0 until dfs.size).map {
            i => colMap.getOrElse(i, t.defaultSeq(dfs(i).lsize).get).asInstanceOf[LSeq[Any]]
          }).asInstanceOf[ColType[Any]]
      }
      override def lsize: Long = {
        MultiDf.this.lsize
      }
    }

  override def openCol[T <: Any](id:ColId) : ColType[T] = {
    val t = types.colType(id)
    val e = entryOfId(id)
    val cols = e.sources zip e.sourceIndexes map { case (source, index) =>
      dfs(source).openCol(index)
    }
    val colMap = (e.sources zip cols).toMap
    t.viewAnyMerged(
      (0 until dfs.size).map {
        i => colMap.getOrElse(i, t.defaultSeq(dfs(i).lsize).get).asInstanceOf[LSeq[Any]]
      }).asInstanceOf[ColType[T]]
  }

  // size in Long
  override lazy val lsize: Long = dfs.map(_.lsize).sum

}

