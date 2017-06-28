package com.futurice.iodf.df

import com.futurice.iodf.ioseq.{IoSeq, IoSeqType}
import com.futurice.iodf.store.RefCounted
import com.futurice.iodf.util._
import com.futurice.iodf.{DfColTypes, IoScope, Utils}

import scala.collection.mutable.ArrayBuffer

object MultiDf {
  def DefaultColIdMemRatio = 16
  def apply[ColId](dfs:Seq[_ <: Df[ColId]], types:DfColTypes[ColId])(
    implicit colIdOrdering:Ordering[ColId]) = {
    new MultiDf[ColId](dfs.toArray, types)
  }
  def refCounted[ColId](dfs:Seq[_ <: RefCounted[Df[ColId]]], types:DfColTypes[ColId])(
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

  def entryOfIndex(index:Long) : Option[MergeSortEntry[ColId]] = {
    if (index < 0 || index >= lsize) {
      None
    } else Some({
      val jumpIndex = (index / colIdMemRatio).toInt
      val jumpEntry = jumpEntries(jumpIndex)

      if (index == jumpIndex) {
        jumpEntry
      } else {
        jumpIterator(jumpEntry).scannedIndex(index).head
      }
    })
  }

  def entryOfId(id: ColId) : Option[MergeSortEntry[ColId]] = {
    if (jumpEntries.isEmpty) {
      None
    } else Some({
      val jumpIndex =
        Utils.binarySearch(LSeq(jumpEntries).map[ColId](_.value), id)(colIdOrdering)._2.toInt
      val jumpEntry = jumpEntries(jumpIndex)

      if (id == jumpEntry.value) {
        jumpEntry
      } else {
        jumpIterator(jumpEntry).scannedValue(id).head
      }
    })
  }


  override def indexOf(id: ColId): Long = {
    entryOfId(id) match {
      case Some(e) if (e.value == id)  => e.index
      case _ => -1
    }
  }

  /*  val colIdEntries = colIdIterator.toArray
  def colIdEntry(i:Long) = colIdEntries(i.toInt)*/

  override val colIds: LSeq[ColId] = {
    new LSeq[ColId] {
      override def apply(l: Long): ColId = {
        entryOfIndex(l).get.value
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

  private def openMultiCol[T](
     entry:Option[MergeSortEntry[ColId]],
     colType : IoSeqType[IoId, _, _ <: LSeq[_], _ <: IoSeq[IoId, _]]) = {
    val colMap =
      entry match {
        case Some(e) =>
          val cols = e.sources zip e.sourceIndexes map { case (source, index) =>
            dfs(source).openCol[T](index)
          }
          (e.sources zip cols).toMap
        case None =>
          Map.empty[Int, LSeq[T]]
      }
    colType.viewAnyMerged(
      (0 until dfs.size).map { i =>
        colMap.getOrElse(i, colType.defaultSeq(dfs(i).lsize).get).asInstanceOf[LSeq[T]]
      }).asInstanceOf[ColType[T]]
  }

  override val _cols: LSeq[ColType[Any]] =
    new LSeq[ColType[Any]] {
      // this will open the column
      override def apply(l: Long): ColType[Any] = {
        openMultiCol[Any](entryOfIndex(l),
                          types.colType(l))
      }
      override def lsize: Long = colIdsLsize
      override def iterator =
        new PeekIterator[ColType[Any]] {
          var at = 0
          val i =
            MergeSortIterator[ColId](seqs.map(_.iterator))
          def head = { // dangerous, as this opens the column
            openMultiCol[Any](
              i.headOption,
              types.colType(at))
          }
          def hasNext = at < colIdsLsize
          def next = {
            val rv = head // opens the column
            if (i.hasNext) i.next
            at += 1
            rv
          }
        }
    }

  override def openCol[T <: Any](id:ColId) : ColType[T] = {
    openMultiCol[T](entryOfId(id),
                    types.colType(id))
  }

  override def view(from:Long, until:Long) = {
    new DfView[IoId, ColId](this, from, until)
  }

  // size in Long
  override lazy val lsize: Long = dfs.map(_.lsize).sum

}

