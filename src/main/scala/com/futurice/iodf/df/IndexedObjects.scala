package com.futurice.iodf.df

import com.futurice.iodf.IoContext
import com.futurice.iodf._
import com.futurice.iodf.io.{DataAccess, DataOutput, MergeableIoType}
import com.futurice.iodf.store.{CfsDir, WrittenCfsDir}
import com.futurice.iodf.util.{LBits, LSeq, Ref}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

/**
  * This is mainly a
  */
class IndexedObjects[T](val _wrappedRef:Ref[Indexed[String, Objects[T]]])
  extends Indexed[String, Objects[T]]
  with ObjectsApi[T] {

  implicit val bind = IoScope.open

  val wrappedRef = _wrappedRef.copy
  val wrapped = wrappedRef.get

  val o = wrapped.df

  override def close = bind.close

  def openAs[E : ClassTag:TypeTag] : Ref[IndexedObjects[E]] = scoped { implicit bind =>
    Ref.open(
      new IndexedObjects[E](
        Ref(
          Indexed(
            wrapped.dfRef.get.as[E],
            wrapped.indexDfRef))))
  }

  def as[E : ClassTag:TypeTag](implicit bind:IoScope) = bind(openAs[E] )

  override def openView(from:Long, until:Long) = scoped { implicit bind =>
    new IndexedObjects[T](Ref(wrapped.openView(from, until)))
  }
  override def openSelect(indexes:LSeq[Long]) = scoped { implicit bind =>
    new IndexedObjects[T](Ref(wrapped.openSelect(indexes)))
  }

  override def df: Objs[T] = wrapped.df

  override def indexDf: Index[String] = wrapped.indexDf

  override def lsize: Long = wrapped.lsize

  override def size : Int = wrapped.size

  override def apply(i: Long): T = o.apply(i)

  override def fieldNames: Array[String] = o.fieldNames

  override def fieldIndexes: Array[Int] = o.fieldIndexes

  override type ColType[T] = wrapped.ColType[T]

  override def schema = wrapped.schema

  override def _cols: LSeq[_ <: ColType[_]] = wrapped._cols
  override def openedIndexes = wrapped.openedIndexes

  override def colIdValues[T](colId: String): LSeq[(String, T)] =
    wrapped.colIdValues(colId)

  override def colIdValuesWithIndex[T](colId: String): Iterable[((String, T), Long)] =
    wrapped.colIdValuesWithIndex(colId)

  override def openIndex(idValue: (String, Any)): LBits =
    wrapped.openIndex(idValue)

  override def openIndex(i: Long): LBits =
    wrapped.openIndex(i)

}

object IndexedObjects {
  def apply[T](indexed:Ref[Indexed[String, Objects[T]]]) : IndexedObjects[T] = {
    new IndexedObjects(indexed)
  }
  def from[T:TypeTag:ClassTag](data:Seq[T], conf:IndexConf[String]) : IndexedObjects[T] = scoped { implicit bind =>
    IndexedObjects(Ref(Indexed.from(Objects.from[T](data), conf)))
  }
  def viewMerged[T:TypeTag](seqs: Seq[Ref[IndexedObjs[T]]], colIdMemRatio:Int = MultiCols.DefaultColIdMemRatio)(implicit io:IoContext)
  : IndexedObjs[T] =
    IndexedObjects(Indexed.openMerged(seqs))
}

class IndexedObjectsIoType[T:TypeTag](
    val objectsIoType: ObjectsIoType[T],
    val indexIoType : IndexIoType[String])(implicit io:IoContext)
  extends MergeableIoType[IndexedObjects[T], IndexedObjects[T]] {

  val indexedIoType = new IndexedIoType[String, Objects[T]](objectsIoType, indexIoType)

  override def interfaceType: universe.Type = typeOf[IndexedObjects[T]]
  override def ioInstanceType: universe.Type = typeOf[IndexedObjects[T]]

  override def open(ref: DataAccess): Ref[IndexedObjects[T]] = scoped { implicit bind =>
    Ref.open(IndexedObjects[T](Ref(indexedIoType.open(ref))))
  }

  override def write(out: DataOutput, iface: Ref[IndexedObjects[T]]): Unit =
    indexedIoType.write(out, iface)

  override def openMerged(seqs: Seq[Ref[IndexedObjects[T]]]): Ref[IndexedObjects[T]] = scoped { implicit bind =>
    Ref.open(IndexedObjects[T](Ref(indexedIoType.openMerged(seqs))))
  }
}