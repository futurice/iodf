package com.futurice.iodf

import com.futurice.iodf.store.RefCounted
import com.futurice.iodf.utils.{MergeSortEntry, MergeSortIterator, PeekIterator, Scanner}

import scala.collection.mutable.ArrayBuffer

object MultiDf {
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
                               types:DfColTypes[IoId, ColId])(
    implicit colIdOrdering:Ordering[ColId]) = {
    val rv = new MultiDf[IoId, ColId](dfs.toArray, types)
    dfs.map { rv.scope.bind(_) }
    rv
  }
}


case class MultiDfEntry(sources:Array[Int], sourceIndexes:Array[Long])

/**
  * Created by arau on 6.6.2017.
  */
class MultiDf[IoId, ColId](dfs:Array[_ <: Df[IoId, ColId]], types:DfColTypes[IoId, ColId])(
  implicit val colIdOrdering:Ordering[ColId]) extends Df[IoId, ColId] {

  type ColType[T] = MultiSeq[T, LSeq[T]]

  val scope = new IoScope()
  /* dfs.foreach { df => scope.bind(df) }*/
  override def close(): Unit = {
    scope.close
  }
  val seqs : Array[LSeq[ColId]] = dfs.map(_.colIds)
  def colIdIterator = new MergeSortIterator[ColId](seqs.map(_.iterator))


  val jumpEntries = new ArrayBuffer[Array[Long]]()
  val jumpValues = new ArrayBuffer[ColId]
  def jumpRatio = 32

  private val colIdsLsize = {
    var at = 0L
    val i = colIdIterator
    while (i.hasNext) {
      if (at % jumpRatio == 0) {
        jumpEntries += i.headIndexes
        jumpValues += i.head.value
      }
      at += 1
      i.next
    }
    at
  }

  def jumpIterator(jumpEntry:Array[Long]) = {
    new MergeSortIterator[ColId](
      (seqs zip jumpEntry).map { case (seq, jump) =>
        Scanner(seq, jump)
      })
  }

  def colIdEntry(index:Long) = {
    val jumpIndex = (index / jumpRatio).toInt
    val jumpStart = jumpIndex * jumpRatio
    val jumpEntry = jumpEntries(jumpIndex)
    val i = jumpIterator(jumpEntry)

    while (i.head.index + jumpStart < index) i.next
    val r = i.head
    MergeSortEntry(
      r.sources,
      (r.sources zip r.sourceIndexes).map{ case (s, i) => jumpEntry(s) + i },
      jumpStart + r.index, r.value)
  }

  def entryOf(id: ColId) = {
    val jumpIndex =
      Utils.binarySearch(LSeq(jumpValues), id)(colIdOrdering)._2.toInt
    val jumpEntry = jumpEntries(jumpIndex)
    val jumpStart = jumpIndex * jumpRatio.toLong
    val i = jumpIterator(jumpEntry)

    while (colIdOrdering.lt(i.head.value, id)) i.next
    val r = i.head
    MergeSortEntry(
      r.sources,
      (r.sources zip r.sourceIndexes).map{ case (s, i) => jumpEntry(s) + i },
      jumpStart + r.index, r.value)
  }


  override def indexOf(id: ColId): Long = {
    val e = entryOf(id)
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
        colIdEntry(l).value
      }
      override def lsize: Long = colIdsLsize
      override def iterator =
        PeekIterator(colIdIterator.map(_.value))
    }
  }

  override val _cols: LSeq[ColType[Any]] =
    new LSeq[ColType[Any]] {
      // this will open the column
      override def apply(l: Long): ColType[Any] = MultiDf.this.synchronized {
        val e = colIdEntry(l)
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
    val e = entryOf(id)
    val colMap = (e.sources zip e.sourceIndexes).toMap
    t.viewAnyMerged(
      (0 until dfs.size).map {
        i => colMap.getOrElse(i, t.defaultSeq(dfs(i).lsize).get).asInstanceOf[LSeq[Any]]
      }).asInstanceOf[ColType[T]]
  }

  // size in Long
  override lazy val lsize: Long = dfs.map(_.lsize).sum

}

