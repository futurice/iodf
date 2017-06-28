package com.futurice.iodf.df

import com.futurice.iodf.Utils._
import com.futurice.iodf.util.LSeq
import com.futurice.iodf.{IoScope, Utils}

import scala.reflect.runtime.universe._


case class ColKey[ColId, Type](colId:ColId) {}

/*
trait SortedIoSeq[IoId, ColId <: Ordered[ColId]] extends IoSeq[IoId, ColId] {
}*/


trait Df[ColId] extends java.io.Closeable {

  def colIds   : LSeq[ColId]
  def colIdOrdering : Ordering[ColId]

  type ColType[T] <: LSeq[T]

  def _cols    : LSeq[ColType[Any]]

  def colCount = colIds.size
  // size in Long
  def lsize : Long

  def indexOf(colId:ColId) =
    Utils.binarySearch(colIds, colId)(colIdOrdering)._1

  def indexFloorAndCeil(id:ColId) =
    Utils.binarySearch(colIds, id)(colIdOrdering)

  def col[T <: Any](id: ColId)(implicit scope:IoScope) : ColType[T] = {
    scope.bind(openCol[T](id))
  }
  def col[T <: Any](i: Long)(implicit scope:IoScope) : ColType[T] = {
    scope.bind(openCol[T](i))
  }
  def col[T <: Any](key:ColKey[ColId, T])(implicit scope:IoScope) = {
    scope.bind(openCol(key.colId))
  }
  def openCol[T <: Any](i:Long) : ColType[T] = {
    _cols(i).asInstanceOf[ColType[T]]
  }
  def apply[T <: Any](i:Long, j:Long) : T = {
    using (openCol[T](i)) { _(j) }
  }
  def openCol[T <: Any](id:ColId) : ColType[T] = {
    indexOf(id) match {
      case -1 => throw new IllegalArgumentException(id + " not found")
      case i => _cols(i).asInstanceOf[ColType[T]]
    }
  }
  def apply[T <: Any](id:ColId, i:Long) : T = {
    using (openCol[T](id)) { _(i) }
  }
  def view(from:Long, until:Long) : Df[ColId]


  /*  def colType[T](i:Long) : IoSeqType[T, _ <: LSeq[T], _ <: IoSeq[T]] = {
      using (openCol(i)) {
        _.asInstanceOf[IoSeq[T]].openRef.typ.asInstanceOf[IoSeqType[T, _ <: LSeq[T], _ <: IoSeq[IoId, T]]]
      }
    }
    def colType[T](id:ColId) : IoSeqType[T, _ <: LSeq[T], _ <: IoSeq[T]] = {
      colType(indexOf(id))
    }*/

}

class DfRef[ColId](val df:Df[ColId]) extends Df[ColId] {
  override def colIds: LSeq[ColId] = df.colIds

  override def colIdOrdering: Ordering[ColId] = df.colIdOrdering

  override type ColType[T] = df.ColType[T]

  override def _cols: LSeq[df.ColType[Any]] = df._cols

  override def lsize: Long = df.lsize

  override def close(): Unit = {}

  override def view(from:Long, until:Long) = df.view(from, until)
}

class DfView[ColId](val df:Df[ColId], val from:Long, val until:Long)
  extends Df[ColId] {

  override def colIds: LSeq[ColId] = df.colIds

  override def colIdOrdering: Ordering[ColId] = df.colIdOrdering

  override type ColType[T] = LSeq[T]

  override def _cols: LSeq[LSeq[Any]] = new LSeq[LSeq[Any]] {
    def apply(l:Long) = df._cols(l).view(from, until)
    def lsize = until - from
  }

  override def lsize: Long = until - from

  override def view(from:Long, until:Long) =
    df.view(this.from + from, this.from + until)

  override def close(): Unit = {}
}

