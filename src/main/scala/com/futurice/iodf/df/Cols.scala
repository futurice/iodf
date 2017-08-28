package com.futurice.iodf.df

import java.io.DataOutputStream

import com.futurice.iodf._
import com.futurice.iodf.io._
import com.futurice.iodf.ioseq.{IoSeq, SeqIoType}
import com.futurice.iodf.store._
import com.futurice.iodf.util.{KeyMap, LSeq, Ref}
import com.futurice.iodf.{IoContext, IoScope, Utils}
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

case class ColKey[ColId, Type](colId:ColId) {}

trait ColSchema[ColId] {
  def colIds : LSeq[ColId]
  def colTypes : LSeq[Type]
  def colMeta : LSeq[KeyMap]
  def colCount = colIds.lsize
}

/*
trait SortedIoSeq[IoId, ColId <: Ordered[ColId]] extends IoSeq[IoId, ColId] {
}*/

object Cols {

  def empty[ColId](lsize:Long)(implicit ord: Ordering[ColId]) =
    apply[ColId](
      LSeq.empty,
      LSeq.empty,
      LSeq.empty,
      LSeq.empty,
      lsize)

  def apply[ColId](_colIds   : LSeq[ColId],
                   _colTypes : LSeq[Type],
                   _colMeta  : LSeq[KeyMap],
                   __cols    : LSeq[_ <: LSeq[_ <: Any]],
                   _lsize    : Long,
                  closer     : () => Unit = () => Unit)(implicit ord: Ordering[ColId]): Cols[ColId] = {
    new Cols[ColId] {
      override type ColType[T] = LSeq[T]
      override def colIds = _colIds
      override def colTypes = _colTypes
      override def colMeta = _colMeta
      override def _cols = __cols.map[ColType[_]](_.asInstanceOf[ColType[_]])
      override def lsize = _lsize
      override def close(): Unit = {
        closer()
      }
      override def colIdOrdering: Ordering[ColId] = ord
    }
  }
}

/**
 * Columns is a collection of A) identified B) typed and C) meta-enriched
 * sequences, that are sorted by id.
 *
 * Columns is the backbone of the iodf's column oriented data structure.
 *
 * As such: the concept of column is separated from a sequence by the fact that:
 *
 *   1. it has an id,
 *   2. it's associated with accurate type information (scala reflection)
 *   3. it has meta information
 *
 */
trait Cols[ColId] extends java.io.Closeable with ColSchema[ColId] {

  type ColType[T] <: LSeq[T]

  def colIds   : LSeq[ColId]
  def colTypes : LSeq[Type]

  def colIdOrdering : Ordering[ColId]

  def _cols    : LSeq[_ <: ColType[_ <: Any]]

  def lsize : Long
  def size : Int = lsize.toInt

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
  def openCol[T <: Any](id:ColId) : ColType[T] = {
    indexOf(id) match {
      case -1 => throw new IllegalArgumentException(id + " not found")
      case i => _cols(i).asInstanceOf[ColType[T]]
    }
  }

  def apply[T <: Any](i:Long, j:Long) : T = {
    using (openCol[T](i)) { _(j) }
  }
  def apply[T <: Any](id:ColId, i:Long) : T = {
    using (openCol[T](id)) { _(i) }
  }
  def view(from:Long, until:Long) : Cols[ColId] = {
    new ColsView[ColId](this, from, until)
  }
  def select(indexes:LSeq[Long]) : Cols[ColId] = {
    Cols[ColId](
      colIds,
      colTypes,
      colMeta,
      _cols.lazyMap { _.select(indexes) },
      indexes.lsize
    )(colIdOrdering)
  }
}


trait IoCols[ColId] extends Cols[ColId] with IoObject {}

object IoCols {
  def apply[ColId](_ioRef:IoRef[IoCols[ColId]],
                   df:Cols[ColId]) = {
    new IoCols[ColId] {
      val ioRef = _ioRef.openCopy
      override def openRef: IoRef[_ <: IoObject] = ioRef.openCopy
      override type ColType[T] = LSeq[T]
      override def colIds: LSeq[ColId] = df.colIds
      override def colTypes: LSeq[universe.Type] = df.colTypes
      override def colMeta: LSeq[KeyMap] = df.colMeta
      override def colIdOrdering: Ordering[ColId] = df.colIdOrdering
      override def _cols: LSeq[_ <: LSeq[_]] = df._cols
      override def lsize: Long = df.lsize
      override def view(from: Long, until: Long): Cols[ColId] = df.view(from, until)
      override def close(): Unit = {
        df.close
        ioRef.close
      }
    }
  }
}


class ColsRef[ColId](val df:Cols[ColId]) extends Cols[ColId] {
  override def colIds: LSeq[ColId] = df.colIds
  override def colTypes: LSeq[universe.Type] = df.colTypes
  override def colMeta = df.colMeta
  override def colIdOrdering: Ordering[ColId] = df.colIdOrdering

  override type ColType[T] = df.ColType[T]

  override def _cols: LSeq[_ <: df.ColType[_]] = df._cols

  override def lsize: Long = df.lsize

  override def close(): Unit = {}

  override def view(from:Long, until:Long) = df.view(from, until)

}

class ColsView[ColId](val cols:Cols[ColId], val from:Long, val until:Long)
  extends Cols[ColId] {

  override def colIds: LSeq[ColId] = cols.colIds
  override def colTypes: LSeq[universe.Type] = cols.colTypes
  override def colMeta = cols.colMeta
  override def colIdOrdering: Ordering[ColId] = cols.colIdOrdering

  override type ColType[T] = LSeq[T]

  override def _cols: LSeq[LSeq[_ <: Any]] = new LSeq[LSeq[_ <: Any]] {
    def apply(l:Long) = cols._cols(l).view(from, until)
    def lsize = until - from
  }

  override def lsize: Long = until - from

  override def view(from:Long, until:Long) =
    cols.view(this.from + from, this.from + until)

  override def close(): Unit = {}

}

/*
class DfIoTypeProvider(implicit val types:IoTypes) extends TypeIoProvider {
  val TypeRef(dfPre, dfSymbol, anyArgs) = typeOf[Df[Any]]
  override def provideWriter(to: universe.Type): Option[IoWriter[_]] = {
    val TypeRef(tPre, tSymbol, tArgs) = to
    if (dfPre == tPre && dfSymbol == tSymbol) {
      val clazz = runtimeMirror(getClass.getClassLoader).runtimeClass(tArgs.head.typeSymbol.asClass)
      new DfIoType( types)
    }
  }
  override def provideOpener(to: universe.Type): Option[IoOpener[_]] = {

  }
}*/

class ColsIoType[ColId:ClassTag:TypeTag:Ordering](implicit val io:IoContext) extends MergeableIoType[Cols[ColId], IoCols[ColId]] {
  val l = LoggerFactory.getLogger(getClass)
  override def interfaceType: universe.Type = typeOf[Cols[ColId]]

  override def ioInstanceType: universe.Type = typeOf[IoCols[ColId]]

  override def open(data: DataAccess): IoCols[ColId] = {
    scoped { implicit bind =>
      val size = data.getBeLong(0)
      val cfsPart = bind(data.openView(8, data.size))
      val dirRef = Ref.open(CfsDir.open[ColId](cfsPart))
      val dir = dirRef.get
      val colIds = dir.list
      val ioCols =
        new LSeq[LSeq[_]] {
          override def apply(l: Long): LSeq[_] =
            io.types.openAs[LSeq[_]](dir.indexRef(l))
          override def lsize: Long = colIds.lsize
        }
      IoCols[ColId](
        IoRef(this, data.dataRef),
        Cols[ColId](
          colIds,
          new LSeq[Type] {
            def lsize = ioCols.lsize
            def apply(i: Long) = // hackish!
              using (ioCols(i)) { _.asInstanceOf[IoSeq[ColId]].seqIoType.valueType }
          },
          LSeq.fill(size, KeyMap.empty),
          ioCols,
          size,
          () => dirRef.close()))
    }
  }
  def writeAsCols(out: DataOutput, size:Long, cols: Iterator[((ColId, Type), LSeq[_])]): Unit = {
    scoped { implicit bind =>
      val before2 = System.currentTimeMillis()
      out.writeLong(size) // write dataframe length explicitly
      val dir = bind(WrittenCfsDir.open[ColId](out))

      l.info("writing columns...")
      val before = System.currentTimeMillis()

      cols.foreach { case ((colId, colType), openedCol) =>
        using (openedCol) { col => // this opens the column
          using(dir.create(colId)) { out =>
            io.types.write(out, col, io.types.toLSeqType(colType))
          }
        }
      }

      l.info("columns written in " + (System.currentTimeMillis() - before) + " ms")
    }
  }
  override def write(out: DataOutput, df: Cols[ColId]): Unit = {
    writeAsCols(out,
                df.lsize,
                (df.colIds.iterator zip
                df.colTypes.iterator zip
                df._cols.iterator))
  }

  override def viewMerged(seqs: Seq[Ref[Cols[ColId]]]): Cols[ColId] = {
    MultiCols.open[ColId](seqs)
  }
}