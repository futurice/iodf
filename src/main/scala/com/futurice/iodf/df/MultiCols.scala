package com.futurice.iodf.df

import com.futurice.iodf.io.{IoTypes, SizedMerging}
import com.futurice.iodf.ioseq.{IoSeq, SeqIoType}
import com.futurice.iodf._
import com.futurice.iodf.util._
import com.futurice.iodf.{IoContext, IoScope, Utils}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

object MultiCols {
  def DefaultColIdMemRatio = 32
  def open[ColId:Ordering](refs:Seq[Ref[_ <: Cols[ColId]]], colIdMemRatio:Int = DefaultColIdMemRatio)(
    implicit io:IoContext) : MultiCols[ColId] = {
    new MultiCols[ColId](refs.toArray, colIdMemRatio)
  }
  def donate[ColId:Ordering](refs:Seq[Ref[_ <: Cols[ColId]]], colIdMemRatio:Int = DefaultColIdMemRatio)(
    implicit io:IoContext) : MultiCols[ColId] = {
    try {
      new MultiCols[ColId](refs.toArray, colIdMemRatio)
    } finally {
      refs.foreach { _.close }
    }
  }
}

class MergedColSchema[ColId](schemas:Array[_ <: ColSchema[ColId]],
                             colIdMemRatio : Int = MultiCols.DefaultColIdMemRatio,
                             closer : () => Unit = () => Unit)(implicit val colIdOrdering:Ordering[ColId])
  extends ColSchema[ColId] {

  val colIdSeqs : Array[LSeq[ColId]] = schemas.map(_.colIds)

  type ColType[T] = MultiSeq[T, LSeq[T]]

  def close = closer()

  // form cache
  val jumpEntries = new ArrayBuffer[MergeSortEntry[ColId]]
  val colIdsLsize = {
    var at = 0L
    val i = MergeSortIterator[ColId](colIdSeqs.map(_.iterator))
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
      (colIdSeqs zip jumpEntry.allSourceIndexes).map { case (seq, jump) =>
        Scanner(seq, jump) : PeekIterator[ColId]
      }, jumpEntry)
  }

  def entryOfIndex(index:Long) : Option[MergeSortEntry[ColId]] = {
    if (index < 0 || index >= colIdsLsize) {
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
        Utils.binarySearch(LSeq.from(jumpEntries).map[ColId](_.value), id)(colIdOrdering)._2.toInt
      val jumpEntry = jumpEntries(jumpIndex)

      if (id == jumpEntry.value) {
        jumpEntry
      } else {
        jumpIterator(jumpEntry).scannedValue(id).head
      }
    })
  }

  def mergeEntries = new LSeq[MergeSortEntry[ColId]] {
    override def apply(l: Long) = {
      entryOfIndex(l).get
    }
    override def lsize = colIdsLsize
    override def iterator =
      PeekIterator.apply[MergeSortEntry[ColId]](
        if (colIdMemRatio == 1) { // special case optimization
          jumpEntries.iterator
        } else {
          MergeSortIterator[ColId](colIdSeqs.map(_.iterator))
        })
  }

  def indexOf(id: ColId): Long = {
    entryOfId(id) match {
      case Some(e) if (e.value == id)  => e.index
      case _ => -1
    }
  }

  val colIds: LSeq[ColId] = mergeEntries.lazyMap(_.value)
  def entryToType(e:MergeSortEntry[ColId]) = {
    schemas(e.sources.head).colTypes(e.sourceIndexes.head)
  }
  val colTypes: LSeq[Type] = mergeEntries.lazyMap(entryToType)
  def entryToMeta(e:MergeSortEntry[ColId]) = {
    schemas(e.sources.head).colMeta(e.sourceIndexes.head)
  }
  val colMeta: LSeq[KeyMap] = mergeEntries.lazyMap(entryToMeta)

}


class JoinedCols[ColId](_refs:Array[Ref[_ <: Cols[ColId]]],
                        override val lsize:Long,
                        val colIdMemRatio: Int = MultiCols.DefaultColIdMemRatio)(
  implicit override val colIdOrdering:Ordering[ColId]) extends Cols[ColId] {

  Tracing.opened(this)

  val dfs = _refs.map(_.get)

  // sanity check
  dfs.foreach { df =>
    if (df.lsize != lsize)
      throw new IllegalArgumentException("Joined data frame length was " + df.lsize + ", expected " + lsize)
  }

  val scope = new IoScope()
  _refs.map(_.copy(scope))

  override val schema =
    new MergedColSchema[ColId](dfs.map(_.schema), colIdMemRatio)

  override def indexOf(id: ColId): Long =
    schema.indexOf(id)

  override val colIds: LSeq[ColId] = schema.colIds
  override val colTypes: LSeq[Type] = schema.colTypes
  override val colMeta: LSeq[KeyMap] = schema.colMeta
  override type ColType[T] = LSeq[T]

  override def _cols = schema.mergeEntries.lazyMap { e =>
    if (e.sources.size != 1)
      throw new IllegalStateException("joined dataframes at indexes " + e.sources.mkString(", ") + " shared a column id " + e.value)
    val df = dfs(e.sources.head)
    df._cols(e.sourceIndexes.head) // there should be exactly one
  }

  override def close() = {
    Tracing.closed(this)
    scope.close
  }
}

/**
  * Created by arau on 6.6.2017.
  */
class MultiCols[ColId](_refs:Array[Ref[_ <: Cols[ColId]]], val colIdMemRatio: Int = MultiCols.DefaultColIdMemRatio)(
  implicit override val colIdOrdering:Ordering[ColId], io:IoContext) extends Cols[ColId] {

  val scope = new IoScope()

  val refs = _refs.map(_.copy(scope))
  val dfs = refs.map(_.get)

  type ColType[T] = MultiSeq[T, LSeq[T]]

  override def close(): Unit = {
    scope.close
  }

  override val schema = new MergedColSchema[ColId](dfs.map(_.schema), colIdMemRatio)

  override def indexOf(id: ColId): Long =
    schema.indexOf(id)

  override val colIds: LSeq[ColId] = schema.colIds
  override val colTypes: LSeq[Type] = schema.colTypes
  override val colMeta: LSeq[KeyMap] = schema.colMeta

  private def openMultiCol[T](entry:MergeSortEntry[ColId]) = scoped { implicit bind =>
    val colMap = {
      val cols = entry.sources zip entry.sourceIndexes map { case (source, index) =>
        Ref(dfs(source).openCol[T](index))
      }
      (entry.sources zip cols).toMap
    }
    val entryType = schema.entryToType(entry)
    val seqType = io.types.seqTypeOf(entryType)
    seqType.viewMerged(
      (0 until dfs.size).map { i =>
        colMap.getOrElse(i,
          Ref(seqType.defaultInstance(dfs(i).lsize).getOrElse {
            throw new RuntimeException(f"${entry} part $i is missing the data, while type $seqType doesn't support it")
          })).asInstanceOf[Ref[LSeq[T]]]
      }).asInstanceOf[ColType[T]]
  }

  override val _cols: LSeq[ColType[Any]] = schema.mergeEntries.lazyMap { e =>
    openMultiCol[Any](e)
  }

  override def openCol[T <: Any](id:ColId) : ColType[T] = {
    openMultiCol[T](schema.entryOfId(id).get)
  }

  override def view(from:Long, until:Long) = {
    new ColsView[ColId](this, from, until)
  }

  // size in Long
  override lazy val lsize: Long = dfs.map(_.lsize).sum

}

