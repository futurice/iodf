package com.futurice.iodf.df

import com.futurice.iodf._
import com.futurice.iodf.Utils._
import com.futurice.iodf.io.{DataAccess, DataOutput, MergeableIoType}
import com.futurice.iodf.util.{KeyMap, LSeq, PeekIterator, Ref}

import scala.collection.immutable.TreeMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

trait Document extends Iterable[(String, Any)] with PartialFunction[String, Any] {
}

object Document {
  def apply(fields:(String, Any)*) = {
    val self = TreeMap[String,Any](fields : _*)
    new Document {
      override def iterator: Iterator[(String, Any)] = self.iterator
      override def isDefinedAt(x: String): Boolean = self.isDefinedAt(x)
      override def apply(v1: String): Any = self.apply(v1)
      override def toString = {
        f"Document(${self.map { case (id, value) => f"$id -> $value" }.mkString(", ")})"
      }
    }
  }
}

trait Documents extends Df[Document] {
  def apply(i:Long) : Document
  def df : Cols[String]
  override def size = lsize.toInt
  override def openView(from:Long, until:Long) =
    Documents(new ColsView[String](df, from, until))
  override def openSelect(indexes:LSeq[Long]) =
    Documents(df.openSelect(indexes))
}

object Documents {

  def optionTypeOf(value:Any) : Type = {
    value match {
      case v:Boolean => typeOf[Option[Boolean]]
      case v:Int => typeOf[Option[Int]]
      case v:Long => typeOf[Option[Long]]
      case v:String => typeOf[Option[String]]
    }
  }

  def from(docs:Seq[Document]) : Documents = apply(docs:_*)

  def apply(docs:Document*) : Documents = {
    val docFields = mutable.Map[String, Type]()
    docs.foreach { d =>
      d.foreach { case (id, value) =>
        docFields.getOrElseUpdate(id, optionTypeOf(value))
      }
    }
    val sortedFields = docFields.toArray.sortBy(_._1)
    val idToIndex = sortedFields.map(_._1).zipWithIndex.toMap

    val colIds = sortedFields.map(_._1)
    val colTypes = sortedFields.map(_._2)
    val cols = Array.fill(docFields.size)(Array.fill[Option[Any]](docs.size)(None))

    val schema =
      ColSchema[String](
        LSeq.from(colIds),
        LSeq.from(colTypes),
        LSeq.fill(sortedFields.length, KeyMap.empty))

    docs.zipWithIndex.foreach { case (doc, index) =>
      doc.foreach { case (id, value) =>
        cols(idToIndex(id))(index) = Some(value)
      }
    }

    val sz = docs.size.toLong

    apply(
      Cols(
        schema,
        new LSeq[LSeq[_]] {
          def apply(i:Long) = LSeq.from(cols(i.toInt))
          def lsize = colIds.size
        },
        sz))
  }

  def apply(d:Cols[String]) : Documents = new Documents() {
    override val df = d
    override def apply(i: Long): Document =
      Document(
        (colIds zip _cols).flatMap { case (colId, openedCol) =>
          val opt =
            using (openedCol) { _(i).asInstanceOf[Option[Any]] }
          opt.map(value => colId -> value)
        }.toSeq : _*)

    override def close = d.close

    override def lsize: Long = df.lsize

    override type ColType[T] = df.ColType[T]

    override val schema = df.schema

    override def _cols: LSeq[_ <: ColType[_]] = df._cols
  }

}

class DocumentsIoType(dfType:ColsIoType[String]) extends MergeableIoType[Documents, Documents] {

  override def interfaceType: universe.Type = typeOf[Documents]

  override def ioInstanceType: universe.Type = typeOf[Documents]

  override def open(ref: DataAccess): Documents = {
    val df = dfType.open(ref)
    Documents(df)
  }

  override def write(out: DataOutput, iface: Documents): Unit = {
    dfType.write(out, iface.df)
  }

  override def openMerged(seqs: Seq[Ref[Documents]]): Documents = {
    // FIXME: this is somewhat problematic situation, because
    //        we cannot access the schema. How can we handle situations
    //        with empty merge list?
    Documents(dfType.openMerged(seqs.map(_.map(_.df))))
  }

}