package com.futurice.iodf.df

import java.io.DataOutputStream

import com.futurice.iodf._
import com.futurice.iodf.io._
import com.futurice.iodf.ioseq.{IoSeq, SeqIoType}
import com.futurice.iodf.store._
import com.futurice.iodf.util.{LSeq, Ref}
import com.futurice.iodf.{IoContext, IoScope, Utils}
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

case class ColKey[ColId, Type](colId:ColId) {}

trait DfSchema[ColId] {
  def colIds : LSeq[ColId]
  def colTypes : LSeq[Type]
  def colCount = colIds.lsize
}

/*
trait SortedIoSeq[IoId, ColId <: Ordered[ColId]] extends IoSeq[IoId, ColId] {
}*/

object Df {

  def apply[ColId](_colIds   : LSeq[ColId],
                   _colTypes : LSeq[Type],
                   __cols    : LSeq[_ <: LSeq[_ <: Any]],
                   _lsize    : Long,
                  closer     : () => Unit = () => Unit)(implicit ord: Ordering[ColId]): Df[ColId] = {
    new Df[ColId] {
      override type ColType[T] = LSeq[T]
      override def colIds = _colIds
      override def colTypes = _colTypes
      override def _cols = __cols.map[ColType[_]](_.asInstanceOf[ColType[_]])
      override def lsize = _lsize
      override def close(): Unit = {
        closer()
      }
      override def view(from:Long, until:Long) = {
        new DfView[ColId](this, from, until)
      }
      override def colIdOrdering: Ordering[ColId] = ord
    }
  }
}

// to change -> Df[ColId, SelfType <: Df[ColId, _ <: SelfType]] ?
// or maybe -> Df[ColId, Row] with apply[Row] and Iterable[Row] and separate Cols[ColId]
trait Df[ColId] extends java.io.Closeable with DfSchema[ColId] {

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
  def view(from:Long, until:Long) : Df[ColId]
}

trait IoDf[ColId] extends Df[ColId] with IoObject {

}

object IoDf {
  def apply[ColId](_ioRef:IoRef[IoDf[ColId]],
                   df:Df[ColId]) = {
    new IoDf[ColId] {
      val ioRef = _ioRef.openCopy
      override def openRef: IoRef[_ <: IoObject] = ioRef.openCopy
      override type ColType[T] = LSeq[T]
      override def colIds: LSeq[ColId] = df.colIds
      override def colTypes: LSeq[universe.Type] = df.colTypes
      override def colIdOrdering: Ordering[ColId] = df.colIdOrdering
      override def _cols: LSeq[_ <: LSeq[_]] = df._cols
      override def lsize: Long = df.lsize
      override def view(from: Long, until: Long): Df[ColId] = df.view(from, until)
      override def close(): Unit = {
        df.close
        ioRef.close
      }
    }
  }
}


class DfRef[ColId](val df:Df[ColId]) extends Df[ColId] {
  override def colIds: LSeq[ColId] = df.colIds
  override def colTypes: LSeq[universe.Type] = df.colTypes
  override def colIdOrdering: Ordering[ColId] = df.colIdOrdering

  override type ColType[T] = df.ColType[T]

  override def _cols: LSeq[_ <: df.ColType[_]] = df._cols

  override def lsize: Long = df.lsize

  override def close(): Unit = {}

  override def view(from:Long, until:Long) = df.view(from, until)

}

class DfView[ColId](val df:Df[ColId], val from:Long, val until:Long)
  extends Df[ColId] {

  override def colIds: LSeq[ColId] = df.colIds
  override def colTypes: LSeq[universe.Type] = df.colTypes
  override def colIdOrdering: Ordering[ColId] = df.colIdOrdering

  override type ColType[T] = LSeq[T]

  override def _cols: LSeq[LSeq[_ <: Any]] = new LSeq[LSeq[_ <: Any]] {
    def apply(l:Long) = df._cols(l).view(from, until)
    def lsize = until - from
  }

  override def lsize: Long = until - from

  override def view(from:Long, until:Long) =
    df.view(this.from + from, this.from + until)

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

class DfIoType[ColId:ClassTag:TypeTag:Ordering](implicit val io:IoContext) extends MergeableIoType[Df[ColId], IoDf[ColId]] {
  val l = LoggerFactory.getLogger(getClass)
  override def interfaceType: universe.Type = typeOf[Df[ColId]]

  override def ioInstanceType: universe.Type = typeOf[IoDf[ColId]]

  override def open(data: DataAccess): IoDf[ColId] = {
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
      IoDf[ColId](
        IoRef(this, data.dataRef),
        Df[ColId](
          colIds,
          new LSeq[Type] {
            def lsize = ioCols.lsize
            def apply(i: Long) = // hackish!
              using (ioCols(i)) { _.asInstanceOf[IoSeq[ColId]].seqIoType.valueType }
          },
          ioCols,
          size,
          () => dirRef.close()))
    }
  }
  def writeAsDf(out: DataOutput, size:Long, cols: Iterator[((ColId, Type), LSeq[_])]): Unit = {
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
  override def write(out: DataOutput, df: Df[ColId]): Unit = {
    writeAsDf(out,
              df.lsize,
              (df.colIds.iterator zip
               df.colTypes.iterator zip
               df._cols.iterator))
  }

  override def viewMerged(seqs: Seq[Ref[Df[ColId]]]): Df[ColId] = {
    MultiDf.open[ColId](seqs)
  }
}