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
class IndexedObjects[T](val wrapped:Indexed[String, Objects[T]])
  extends Indexed[String, Objects[T]]
  with ObjectsApi[T] {

  private val o = wrapped.df

  override def close = wrapped.close

  def as[E : ClassTag:TypeTag] : IndexedObjects[E] = {
    new IndexedObjects[E](
      Indexed(
        wrapped.df.as[E],
        wrapped.indexDf))
  }

  override def view(from:Long, until:Long) =
    new IndexedObjects[T](wrapped.view(from, until))

  override def df: Objs[T] = wrapped.df

  override def indexDf: Index[String] = wrapped.indexDf

  override def lsize: Long = wrapped.lsize

  override def size : Int = wrapped.size

  override def apply(i: Long): T = o.apply(i)

  override def fieldNames: Array[String] = o.fieldNames

  override def fieldIndexes: Array[Int] = o.fieldIndexes

  override type ColType[T] = wrapped.ColType[T]

  override def colIds: LSeq[String] = wrapped.colIds

  override def colTypes: LSeq[universe.Type] = wrapped.colTypes

  override def colIdOrdering: Ordering[String] = wrapped.colIdOrdering

  override def _cols: LSeq[_ <: ColType[_]] = wrapped._cols

  override def colNameValues[T](colId: String): LSeq[(String, T)] =
    wrapped.colNameValues(colId)

  override def colNameValuesWithIndex[T](colId: String): Iterable[((String, T), Long)] =
    wrapped.colNameValuesWithIndex(colId)

  override def openIndex(idValue: (String, Any)): LBits =
    wrapped.openIndex(idValue)

  override def openIndex(i: Long): LBits =
    wrapped.openIndex(i)

}

object IndexedObjects {
  def apply[T](indexed:Indexed[String, Objects[T]]) : IndexedObjects[T] = {
    new IndexedObjects(indexed)
  }
  def from[T:TypeTag:ClassTag](data:Seq[T], conf:IndexConf[String]) : IndexedObjects[T] = {
    IndexedObjects(Indexed.from(Objects.from(data), conf))
  }
  def viewMerged[T:TypeTag](seqs: Seq[Ref[IndexedObjs[T]]], colIdMemRatio:Int = MultiDf.DefaultColIdMemRatio)(implicit io:IoContext)
  : IndexedObjs[T] =
    IndexedObjects(Indexed.viewMerged(seqs))
}

class IndexedObjectsIoType[T:TypeTag](
    val objectsIoType: ObjectsIoType[T],
    val indexIoType : IndexIoType[String])(implicit io:IoContext)
  extends MergeableIoType[IndexedObjects[T], IndexedObjects[T]] {

  val indexedIoType = new IndexedIoType[String, Objects[T]](objectsIoType, indexIoType)

  override def interfaceType: universe.Type = typeOf[IndexedObjects[T]]
  override def ioInstanceType: universe.Type = typeOf[IndexedObjects[T]]

  override def open(ref: DataAccess): IndexedObjs[T] =
    IndexedObjects(indexedIoType.open(ref))

  override def write(out: DataOutput, iface: IndexedObjs[T]): Unit =
    indexedIoType.write(out, iface)

  override def viewMerged(seqs: Seq[Ref[IndexedObjs[T]]]): IndexedObjs[T] =
    IndexedObjects(indexedIoType.viewMerged(seqs))
}