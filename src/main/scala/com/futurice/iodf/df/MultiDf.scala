package com.futurice.iodf.df

import com.futurice.iodf.io.SizedMerging
import com.futurice.iodf.ioseq.{IoSeq, SeqIoType}
import com.futurice.iodf.util._
import com.futurice.iodf.{IoScope, Utils}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

object MultiDf {
  def DefaultColIdMemRatio = 16
  def apply[ColId](dfs:Seq[_ <: Df[ColId]], types:DfMerging[ColId])(
    implicit colIdOrdering:Ordering[ColId]) = {
    new MultiDf[ColId](dfs.toArray, types)
  }
  def refCounted[ColId](dfs:Seq[_ <: Ref[Df[ColId]]], types:DfMerging[ColId])(
    implicit colIdOrdering:Ordering[ColId]) = {
    val rv = new MultiDf[ColId](dfs.map(_.get).toArray, types)
    dfs.map { rv.scope.bind(_) }
    rv
  }
  def autoClosing[ColId](dfs:Seq[_ <: Df[ColId]],
                         types:DfMerging[ColId], colIdMemRatio:Int = DefaultColIdMemRatio)(
    implicit colIdOrdering:Ordering[ColId]) = {
    val rv = new MultiDf[ColId](dfs.toArray, types, colIdMemRatio)
    dfs.map { rv.scope.bind(_) }
    rv
  }
}


/**
  * Created by arau on 6.6.2017.
  */
class MultiDf[ColId](dfs:Array[_ <: Df[ColId]], types:DfMerging[ColId], val colIdMemRatio: Int = 32)(
  implicit val colIdOrdering:Ordering[ColId]) extends Df[ColId] {

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
        PeekIterator.apply[ColId](
          if (colIdMemRatio == 1) { // special case optimization
            jumpEntries.map(_.value).iterator
          } else {
            MergeSortIterator[ColId](seqs.map(_.iterator)).map(_.value)
        })
    }
  }

  def entryToType(e:MergeSortEntry[ColId]) = {
    dfs(e.sources.head).colTypes(e.sourceIndexes.head)
  }

  override val colTypes: LSeq[Type] = {
    new LSeq[Type] {
      override def apply(l: Long): Type = {
        entryToType(entryOfIndex(l).get)
      }
      override def lsize: Long = colIdsLsize
      override def iterator =
        PeekIterator.apply[Type](
          if (colIdMemRatio == 1) { // special case optimization
            jumpEntries.map( entryToType ).iterator
          } else {
            MergeSortIterator[ColId](seqs.map(_.iterator)).map( entryToType )
          })
    }
  }

  private def openMultiCol[T](
     entry:Option[MergeSortEntry[ColId]],
     colMerging : SizedMerging[LSeq[T]]) = {
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
    colMerging.viewMerged(
      (0 until dfs.size).map { i =>
        colMap.getOrElse(i,
          colMerging.defaultInstance(dfs(i).lsize).getOrElse {
            throw new RuntimeException(f"${entry} part $i is missing the data, while type $colMerging doesn't support it")
          }).asInstanceOf[LSeq[T]]
      }).asInstanceOf[ColType[T]]
  }

  override val _cols: LSeq[ColType[Any]] =
    new LSeq[ColType[Any]] {
      // this will open the column
      override def apply(l: Long): ColType[Any] = {
        openMultiCol[Any](entryOfIndex(l),
                          types.colMerging(l).asInstanceOf[SizedMerging[LSeq[Any]]])
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
              types.colMerging(at).asInstanceOf[SizedMerging[LSeq[Any]]])
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
                    types.colMerging(id).asInstanceOf[SizedMerging[LSeq[T]]])
  }

  override def view(from:Long, until:Long) = {
    new DfView[ColId](this, from, until)
  }

  // size in Long
  override lazy val lsize: Long = dfs.map(_.lsize).sum

}

