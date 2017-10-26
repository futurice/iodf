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

  def openSelectCols(indexes:LSeq[Long]) = scoped { implicit bind =>
    Ref.open(
      ColSchema(colIds.select(indexes),
                colTypes.select(indexes),
                colMeta.select(indexes),
                bind.adopt())(colIdOrdering))
  }

  def selectCols(indexes:LSeq[Long])(implicit bind:IoScope) = {
    bind(openSelectCols(indexes))
  }
}

object ColSchema {
  def empty[ColId](implicit ord: Ordering[ColId]) =
    apply[ColId](LSeq.emptyRef, LSeq.emptyRef, LSeq.emptyRef)

  def apply[ColId](_colIds:Ref[LSeq[ColId]],
                   _colTypes:Ref[LSeq[Type]],
                   _colMeta:Ref[LSeq[KeyMap]],
                   closer : Closeable = Utils.dummyCloseable)(
                   implicit ord: Ordering[ColId]) = {
    new ColSchema[ColId] {
      implicit val bind = IoScope.open
      bind(closer)
      override val colIds   = _colIds.copy.get
      override val colTypes = _colTypes.copy.get
      override val colMeta  = _colMeta.copy.get
      override def colIdOrdering: Ordering[ColId] = ord
      override def close = bind.close()
    }
  }
}

/*
trait SortedIoSeq[IoId, ColId <: Ordered[ColId]] extends IoSeq[IoId, ColId] {
}*/

object Cols {

  def empty[ColId](lsize:Long)(implicit ord: Ordering[ColId]) =
    apply[ColId](
      Ref.mock(ColSchema.empty[ColId]),
      Ref.mock(LSeq.empty),
      lsize)

  /**
    * From schema & columns
    */
  def apply[ColId](_schemaRef:Ref[ColSchema[ColId]],
                   __colsRef:Ref[LSeq[_ <: LSeq[_ <: Any]]],
                   _lsize : Long,
                   closer : Closeable = Utils.dummyCloseable) : Cols[ColId] = {
    new Cols[ColId] {
      implicit val bind = IoScope.open
      override val schemaRef = _schemaRef.copy
      override val _colsRef = __colsRef.copy
      bind(closer)
      override type ColType[T] = LSeq[T]
      override def schema = schemaRef.get
      override val _cols = _colsRef.get.lazyMap(_.asInstanceOf[ColType[_]])
      override def lsize = _lsize
      override def close(): Unit = {
        bind.close
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

  def schemaRef : Ref[ColSchema[ColId]]
  def schema : ColSchema[ColId] = schemaRef.get

  def colIds   : LSeq[ColId] = schema.colIds
  def colTypes : LSeq[Type] = schema.colTypes
  def colMeta : LSeq[KeyMap] = schema.colMeta
  def colCount = schema.colCount
  def colIdOrdering : Ordering[ColId] = schema.colIdOrdering

  def _colsRef : Ref[LSeq[_ <: ColType[_ <: Any]]]
  def _cols    : LSeq[_ <: ColType[_ <: Any]] = _colsRef.get

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
  def openView(from:Long, until:Long) : Ref[Cols[ColId]] = {
    Ref.open(new ColsView[ColId](this, from, until))
  }
  def view(from:Long, until:Long)(implicit bind:IoScope) = bind(openView(from, until))
  /* selects rows */
  def openSelect(indexes:LSeq[Long]) : Ref[Cols[ColId]] = {
    Ref.open(
      Cols[ColId](
        schemaRef,
        _colsRef.map(_.lazyMap { _.openSelect(indexes).get }),
        indexes.lsize))
  }
  def select(indexes:LSeq[Long])(implicit bind:IoScope) = bind(openSelect(indexes))
  /* selects cols*/
  def openSelectCols(indexes:LSeq[Long]) : Ref[Cols[ColId]] = {
    implicit val bind = IoScope.open
    Ref.open(
      Cols[ColId](
        schema.selectCols(indexes),
        _cols.select(indexes),
        lsize,
        bind))
  }
  def selectCols(indexes:LSeq[Long])(implicit bind:IoScope) = bind(openSelectCols(indexes))
}

trait ColsWrap[ColId, T <: Cols[ColId]] {
  def wrappedCols : T
}

class ColsRef[ColId](val cols:Cols[ColId]) extends Cols[ColId] {

  override type ColType[T] = cols.ColType[T]

  override def schemaRef = cols.schemaRef

  override def _colsRef = cols._colsRef

  override def lsize: Long = cols.lsize

  override def close(): Unit = {}

  override def openView(from:Long, until:Long) = cols.openView(from, until)

}

class ColsView[ColId](val cols:Cols[ColId], val from:Long, val until:Long)
  extends Cols[ColId] {


  override type ColType[T] = LSeq[T]

  override def schemaRef = cols.schemaRef

  override val _colsRef = Ref.open(new LSeq[LSeq[_ <: Any]] {
    def apply(l:Long) = cols._cols(l).view(from, until)
    def lsize = until - from
  })

  override def lsize: Long = until - from

  override def openView(from:Long, until:Long) =
    cols.openView(this.from + from, this.from + until)

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
      val typeIds = dir.ref(1).as[LSeq[Int]].map(_.lazyMap { i =>
        io.types.idIoType(i).interfaceType
      })
      val metas = dir.ref(2).as[LSeq[KeyMap]]

      ColSchema[ColId](colIds, typeIds, metas, resources.adopt)
    }
  }

  override def write(out: DataOutput, v: Ref[ColSchema[ColId]]): Unit = scoped { implicit bind =>
    using (WrittenOrderDir.open(out)) { dir =>
      io.save(dir.ref(0), Ref.mock(v.get.colIds))
      io.save(dir.ref(1), Ref.mock(v.get.colTypes.lazyMap { t => io.types.typeId(t) } ))
      io.save(dir.ref(2), Ref.mock(v.get.colMeta))
    }
  }

  override def openMerged(seqs: Seq[Ref[ColSchema[ColId]]]) = {
    val openRefs = seqs.map(_.openCopy)
    seqs.size match {
      case 0 => Ref.open(ColSchema.empty)
      case 1 => seqs.head.openCopy
      case _ =>
        Ref.open(
          new MergedColSchema[ColId](
            openRefs.map(_.get).toArray,
            MultiCols.DefaultColIdMemRatio,
            () => openRefs.foreach(_.close())))
    }
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
          io.types.save(dir.lastRef, Ref.mock(seq), io.types.toLSeqType(tpe))
        }
      }
    }
  }

  override def write(out: DataOutput, v: Ref[LSeq[(Type, LSeq[Any])]]): Unit =
    writeStream(out, v.get.iterator)

}


class ColsIoType[ColId:ClassTag:TypeTag:Ordering](implicit val io:IoContext)
  extends MergeableIoType[Cols[ColId], Cols[ColId]] {
  val l = LoggerFactory.getLogger(getClass)

  val colIdOrd = implicitly[Ordering[ColId]]

  override def interfaceType: universe.Type = typeOf[Cols[ColId]]
  override def ioInstanceType: universe.Type = typeOf[Cols[ColId]]

  override def open(data: DataAccess): Ref[Cols[ColId]] = {
    scoped { implicit resources =>
      val dir = OrderDir(data)
      Ref.open(
        Cols[ColId](
          dir.ref(2).as[ColSchema[ColId]],
          dir.ref(0).as[LSeq[(Type, LSeq[Any])]].map(_.lazyMap(_._2)), // columns are written first, because
          dir.ref(1).as[Long].get,
          resources.adopt))
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
    dir.ref(1).save(Ref.mock(lsize))
    dir.ref(2).save(
      Ref.mock(
        ColSchema[ColId](
          Ref.mock(lmeta.lazyMap(_._1)),
          Ref.mock(lmeta.lazyMap(_._2)),
          Ref.mock(lmeta.lazyMap(_._3)))))
  }

  override def write(out: DataOutput, df: Ref[Cols[ColId]]): Unit = scoped { implicit bind =>
    val dir = WrittenOrderDir(out)
    dir.ref(0).save(
      Ref.mock(
        df.get.schema.colTypes zip df.get._cols.asInstanceOf[LSeq[LSeq[Any]]] : LSeq[(Type, LSeq[Any])]))
    dir.ref(1).save(Ref.mock(df.get.lsize))
    dir.ref(2).save(Ref.mock(df.get.schema))
  }

  override def openMerged(seqs: Seq[Ref[Cols[ColId]]]): Ref[Cols[ColId]] = {
    Ref.open(MultiCols.open[ColId](seqs))
  }
}
