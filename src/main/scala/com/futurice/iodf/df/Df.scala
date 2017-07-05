package com.futurice.iodf.df

import java.io.DataOutputStream

import com.futurice.iodf.Utils._
import com.futurice.iodf.io.{SizedMerging, _}
import com.futurice.iodf.ioseq.{IoSeq, SeqIoType}
import com.futurice.iodf.store._
import com.futurice.iodf.util.{LSeq, Ref}
import com.futurice.iodf.{IoScope, Utils}
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._


case class ColKey[ColId, Type](colId:ColId) {}

/*
trait SortedIoSeq[IoId, ColId <: Ordered[ColId]] extends IoSeq[IoId, ColId] {
}*/

object Df {

  def emptyIoSchema[ColId] = {
    new DfMerging[ColId] {
      override def colMerging(col: ColId) = {
        throw new IllegalArgumentException
      }

      override def colMerging(index: Long) = {
        throw new IllegalArgumentException
      }
    }
  }
  def ioSchema[ColId](df:Df[ColId])(implicit types: IoTypes) = {
    new DfMerging[ColId] {
      def colMerging(colId: ColId) : SizedMerging[LSeq[_]] = {
        colMerging(df.indexOf(colId))
      }
      def colMerging(index: Long) : SizedMerging[LSeq[_]] = {
        types.seqTypeOf(df.colTypes(index))
      }
    }
  }

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
        _colIds.close;
        __cols.close
        closer()
      }
      override def view(from:Long, until:Long) = {
        new DfView[ColId](this, from, until)
      }
      override def colIdOrdering: Ordering[ColId] = ord
    }
  }

}

/*
 * This class is a bit problematic.. We basically need some kind of schema object for the multi-df when:
 *
 *   A) merging columns
 *   B) writing columns
 *
 * The challenge with the schema class is that there are basically three kinds of schemas:
 *
 *  1. Fixed columns with fixed types (typed Df)
 *  2. Dynamic columns with fixed type (df index)
 *  3. Dynamic columns with dynamic types (normal df)
 *
 */
trait DfMerging[ColId] {
  def colMerging(col:ColId) : SizedMerging[_ <: LSeq[_]]
  def colMerging(index:Long) : SizedMerging[_ <: LSeq[_]]
}

trait Df[ColId] extends java.io.Closeable {

  type ColType[T] <: LSeq[T]

  def colIds   : LSeq[ColId]
  def colTypes : LSeq[Type]

  def colIdOrdering : Ordering[ColId]

  def _cols    : LSeq[_ <: ColType[_ <: Any]]

  def colCount = colIds.size

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
      val ioRef = _ioRef.copy
      override def openRef: IoRef[_ <: IoObject] = ioRef.copy
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

class DfIoType[ColId:ClassTag:TypeTag:Ordering](implicit val types:IoTypes) extends MergeableIoType[Df[ColId], IoDf[ColId]] {
  val l = LoggerFactory.getLogger(getClass)
  override def interfaceType: universe.Type = typeOf[Df[ColId]]

  override def ioInstanceType: universe.Type = typeOf[IoDf[ColId]]

  override def open(data: DataAccess): IoDf[ColId] = {
    scoped { implicit bind =>
      val size = data.getBeLong(0)
      val cfsPart = bind(data.openView(8, data.size))
      val dirRef = bind(Ref.open(CfsDir.open[ColId](cfsPart)))
      val dir = dirRef.get
      val colIds = dir.list
      val ioCols =
        new LSeq[LSeq[_]] {
          override def apply(l: Long): LSeq[_] =
            types.openAs[LSeq[_]](dir.indexRef(l))
          override def lsize: Long = colIds.lsize
        }
      IoDf[ColId](
        IoRef.open(this, data.dataRef),
        Df[ColId](
          colIds,
          new LSeq[Type] {
            def lsize = ioCols.lsize
            def apply(i: Long) = // hackish!
              ioCols(i).asInstanceOf[IoSeq[ColId]].seqIoType.valueType
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
            types.write(out, col, types.toLSeqType(colType))
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

  override def viewMerged(seqs: Seq[Df[ColId]]): Df[ColId] = {
    val schema = seqs.headOption.map(Df.ioSchema(_)).getOrElse(Df.emptyIoSchema[ColId])
    MultiDf[ColId](seqs, schema)
  }
}