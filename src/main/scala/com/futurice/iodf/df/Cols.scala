package com.futurice.iodf.df

import java.io.{Closeable, DataOutputStream}

import com.futurice.iodf._
import com.futurice.iodf.io._
import com.futurice.iodf.ioseq.{IoSeq, SeqIoType}
import com.futurice.iodf.store._
import com.futurice.iodf.util.{KeyMap, LSeq, Ref}
import com.futurice.iodf.{IoContext, IoScope, Utils}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

case class ColKey[ColId, Type](colId:ColId) {}

trait ColSchema[ColId] extends Closeable {
  def colIds : LSeq[ColId]
  /** FIXME: should this be renamed to colMemberTypes */
  def colTypes : LSeq[Type]
  def colMeta : LSeq[KeyMap]
  def colIdOrdering : Ordering[ColId]
  def colCount = colIds.lsize
}

object ColSchema {
  def empty[ColId](implicit ord: Ordering[ColId]) =
    apply[ColId](LSeq.empty, LSeq.empty, LSeq.empty)

  def apply[ColId](_colIds:LSeq[ColId],
                   _colTypes:LSeq[Type],
                   _colMeta:LSeq[KeyMap],
                   closer : Closeable = Utils.dummyCloseable)(
                   implicit ord: Ordering[ColId]) = {
    new ColSchema[ColId] {
      override def colIds   = _colIds
      override def colTypes = _colTypes
      override def colMeta  = _colMeta
      override def colIdOrdering: Ordering[ColId] = ord
      override def close = closer.close()
    }
  }
}

/*
trait SortedIoSeq[IoId, ColId <: Ordered[ColId]] extends IoSeq[IoId, ColId] {
}*/

object Cols {

  def empty[ColId](lsize:Long)(implicit ord: Ordering[ColId]) =
    apply[ColId](
      ColSchema.empty[ColId],
      LSeq.empty,
      lsize)

  /**
    * From schema & columns
    */
  def apply[ColId](_schema:ColSchema[ColId],
                   __cols:LSeq[_ <: LSeq[_ <: Any]],
                   _lsize : Long,
                   closer : Closeable = Utils.dummyCloseable) : Cols[ColId] = {
    new Cols[ColId] {
      override type ColType[T] = LSeq[T]
      override def schema = _schema
      override def _cols = __cols.map[ColType[_]](_.asInstanceOf[ColType[_]])
      override def lsize = _lsize
      override def close(): Unit = {
        closer.close()
      }
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
trait Cols[ColId] extends java.io.Closeable {

  type ColType[T] <: LSeq[T]

  def schema : ColSchema[ColId]

  def colIds   : LSeq[ColId] = schema.colIds
  def colTypes : LSeq[Type] = schema.colTypes
  def colMeta : LSeq[KeyMap] = schema.colMeta
  def colCount = schema.colCount
  def colIdOrdering : Ordering[ColId] = schema.colIdOrdering

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
      schema,
      _cols.lazyMap { _.select(indexes) },
      indexes.lsize)
  }
}

class ColsRef[ColId](val df:Cols[ColId]) extends Cols[ColId] {

  override def schema = df.schema

  override type ColType[T] = df.ColType[T]

  override def _cols: LSeq[_ <: df.ColType[_]] = df._cols

  override def lsize: Long = df.lsize

  override def close(): Unit = {}

  override def view(from:Long, until:Long) = df.view(from, until)

}

class ColsView[ColId](val cols:Cols[ColId], val from:Long, val until:Long)
  extends Cols[ColId] {

  override def schema = cols.schema

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

class ColSchemaIoType[ColId:ClassTag:TypeTag:Ordering](implicit val io:IoContext)
  extends MergeableIoType[ColSchema[ColId], ColSchema[ColId]] {

  val l = LoggerFactory.getLogger(getClass)

  override def interfaceType: universe.Type = typeOf[ColSchema[ColId]]
  override def ioInstanceType: universe.Type = typeOf[ColSchema[ColId]]

  override def open(data: DataAccess): ColSchema[ColId] = {
    scoped { implicit resources =>
      val dir = OrderDir(data)

      val colIds = dir.ref(0).as[LSeq[ColId]]
      val typeIds = dir.ref(1).as[LSeq[Int]].lazyMap { i =>
        io.types.idIoType(i).interfaceType
      }
      val metas = dir.ref(2).as[LSeq[KeyMap]]

      ColSchema[ColId](colIds, typeIds, metas, resources.adopt)
    }
  }

  override def write(out: DataOutput, v: ColSchema[ColId]): Unit = scoped { implicit bind =>
    using (WrittenOrderDir.open(out)) { dir =>
      io.save(dir.ref(0), v.colIds)
      io.save(dir.ref(1), v.colTypes.lazyMap { t => io.types.typeId(t) } )
      io.save(dir.ref(2), v.colMeta)
    }
  }

  override def viewMerged(seqs: Seq[Ref[ColSchema[ColId]]]) = {
    val openRefs = seqs.map(_.openCopy)
    new MergedColSchema[ColId](
      openRefs.map(_.get).toArray,
      MultiCols.DefaultColIdMemRatio,
      () => openRefs.foreach(_.close()))
  }
}


class ColIoType(implicit val io:IoContext)
  extends IoType[LSeq[(Type, LSeq[Any])],
                 LSeq[(Type, LSeq[Any])]] {

  val l = LoggerFactory.getLogger(getClass)

  override def interfaceType: universe.Type = typeOf[LSeq[(Type, LSeq[Any])]]
  override def ioInstanceType: universe.Type = typeOf[LSeq[(Type, LSeq[Any])]]

  override def open(data: DataAccess): LSeq[(Type, LSeq[Any])] = {
    val dir = OrderDir.open(data)
    new LSeq[(Type, LSeq[Any])] {
      override def apply(l: Long) : (Type, LSeq[Any]) = scoped { implicit bind =>
        val data = dir.ref(l.toInt).access
        val ioType = io.types.ioTypeOf(data).asInstanceOf[SeqIoType[_, _, _]]
        (ioType.valueType,
         io.types.open(data).asInstanceOf[LSeq[Any]])
      }
      override def lsize = dir.lsize
      override def close = dir.close
    }
  }

  def writeStream(out: DataOutput, i: Iterator[(Type, LSeq[Any])]): Unit = scoped { implicit bind =>
    using (WrittenOrderDir.open(out)) { dir =>
      while (i.hasNext) {
        val (tpe, openedSeq) = i.next
        using (openedSeq) { seq =>
          io.types.save(dir.lastRef, seq, io.types.toLSeqType(tpe))
        }
      }
    }
  }

  override def write(out: DataOutput, v: LSeq[(Type, LSeq[Any])]): Unit =
    writeStream(out, v.iterator)

}


/*
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
                (df.colIds.iterator
                 zip df.colTypes.iterator
                 zip df._cols.iterator))
  }

  override def viewMerged(seqs: Seq[Ref[Cols[ColId]]]): Cols[ColId] = {
    MultiCols.open[ColId](seqs)
  }
}
*/

class ColsIoType[ColId:ClassTag:TypeTag:Ordering](implicit val io:IoContext)
  extends MergeableIoType[Cols[ColId], Cols[ColId]] {
  val l = LoggerFactory.getLogger(getClass)

  val colIdOrd = implicitly[Ordering[ColId]]

  override def interfaceType: universe.Type = typeOf[Cols[ColId]]
  override def ioInstanceType: universe.Type = typeOf[Cols[ColId]]

  override def open(data: DataAccess): Cols[ColId] = {
    scoped { implicit resources =>
      val dir = OrderDir(data)
      Cols[ColId](
        dir.ref(2).as[ColSchema[ColId]],
        dir.ref(0).as[LSeq[(Type, LSeq[Any])]].lazyMap(_._2), // columns are written first, because
        dir.ref(1).as[Long],
        resources.adopt)
    }
  }

  def writeStream(out: DataOutput, lsize:Long, colStream: Iterator[((ColId, Type, KeyMap), LSeq[_])]) : Unit = scoped { implicit bind =>
    val dir = WrittenOrderDir(out)

    val meta = ArrayBuffer[(ColId, Type, KeyMap)]()

    // NOTE: this is hackish. We assume very strict usage pattern, that may fail miserable
    //       on different usages
    dir.ref(0).save(new LSeq[(Type, LSeq[Any])] {
      var used = false
      override def apply(l: Long) = throw new RuntimeException("unsupported operation")
      override def lsize = throw new RuntimeException("unsupported operation")
      override def iterator = {
        if (used) throw new IllegalStateException("iterator can only be called once")
        used = true
        colStream.map { i =>
          meta += i._1
          (i._1._2, i._2)
        }
      }
    })
    val lmeta = LSeq.from(meta)
    dir.ref(1).save(lsize)
    dir.ref(2).save(
      ColSchema[ColId](
        lmeta.lazyMap(_._1),
        lmeta.lazyMap(_._2),
        lmeta.lazyMap(_._3)))
  }

  override def write(out: DataOutput, df: Cols[ColId]): Unit = scoped { implicit bind =>
    val dir = WrittenOrderDir(out)
    dir.ref(0).save(
      df.schema.colTypes zip df._cols.asInstanceOf[LSeq[LSeq[Any]]] : LSeq[(Type, LSeq[Any])])
    dir.ref(1).save(df.lsize)
    dir.ref(2).save(df.schema)
  }

  override def viewMerged(seqs: Seq[Ref[Cols[ColId]]]): Cols[ColId] = {
    MultiCols.open[ColId](seqs)
  }
}
