package com.futurice.iodf.io

import java.io.{BufferedOutputStream, DataOutputStream}

import com.futurice.iodf.Utils.using
import com.futurice.iodf.ioseq._
import com.futurice.iodf.store.{DataCreatorRef, DataRef, FileRef, RandomAccess}
import com.futurice.iodf.util.{LBits, LSeq}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

/**
 * Few thoughts..
 *
 *   it would be nice to have version of write / open that
 *   will write down the object type.
 *
 *   In this case, you could
 *
 */

trait IoTypes {
  def orderingOf(valueTypeTag: TypeTag[_]) : Ordering[Any]

/*  def ioType = new IoType[Any] {
    override def valueType: universe.Type = typeOf[Any]
    override def open(data: DataRef): Any =
      open[]()
    override def write(out: DataOutputStream, iface: Any): Unit = ???
  }*/

  def ioTypeOf(t : Type) : IoType[Any]
  def ioTypeOf[T : TypeTag]() : IoType[T]

  def write[Interface](ref:DataOutputStream, t:Interface)(
    implicit tag:TypeTag[Interface])
  def create[Interface](ref:DataCreatorRef, t:Interface)(
    implicit tag:TypeTag[Interface]) : Interface
  def open[Interface](ref:DataRef)(implicit tag:TypeTag[Interface])
    : Interface

  def getIoTypeId(t:IoType[_]) : Option[Int]

  def ioTypeId(t:IoType[_]) = {
    getIoTypeId(t) match {
      case Some(id) => id
      case None => throw new RuntimeException("type " + t + " of ClassLoader " + t.getClass.getClassLoader + " not found for type scheme " + this + " of ClassLoader " + IoTypes.this.getClass.getClassLoader)
    }
  }
  def idIoType(i:Int) : IoType[_]

  // shortcuts: reconsider
//  def idOrdering : Ordering[IoId]
  def anyOrdering : Ordering[Any]

  def seqTypeOf[Member](implicit typeTag:TypeTag[LSeq[Member]])
    : IoSeqType[Member, LSeq[Member],_ <: IoSeq[Member]] = {
    ioTypeOf[LSeq[Member]].asInstanceOf[IoSeqType[Member, LSeq[Member],_ <: IoSeq[Member]]]
  }

  def longSeqType = seqTypeOf[Long]
//  def idSeqType : IoSeqType[IoId, LSeq[IoId],_ <: IoSeq[IoId, IoId]]
  def refSeqType : IoSeqType[IoObject, LSeq[IoObject],_ <: IoSeq[IoObject]]
  def bitsSeqType : IoSeqType[Boolean, LBits,_ <: IoSeq[Boolean]] = {
    ioTypeOf[LBits].asInstanceOf[IoSeqType[Boolean, LBits,_ <: IoSeq[Boolean]]]
  }
}

abstract class Bounds() {}
case class MinBound() extends Bounds() {}
case class MaxBound() extends Bounds() {}

object IoTypes {
  type strSeqType = StringIoSeqType[String]

  def apply(types:Seq[IoType[_]]) = {
    new IoTypes {
      def entryOf(t:Type) : (IoType[_ <: IoObject], Int) = {
        types.toStream
             .zipWithIndex
             .flatMap(e => e._1.cast(t).map(c => (c, e._2)))
             .headOption match {
          case Some(v) => v
          case None =>
            throw new RuntimeException("no io type for " + t + " / " + t.hashCode() + ". ")
        }
      }
      def entryOf[T : TypeTag] : (IoType[T], Int) = {
        entryOf(typeOf[T]).asInstanceOf[(IoType[T], Int)]
      }
      def ioTypeOf(t:Type) : IoType[_ <: IoObject] = {
        entryOf(t)._1
      }
      def ioTypeOf[T : TypeTag]() : IoType[T] = {
        ioTypeOf(typeOf[T]).asInstanceOf[IoType[T]]
      }
      def write[From : TypeTag](out:DataOutputStream, v:From) = {
        val (typ, id) = entryOf[From]
        out.writeInt(id)
        typ.write(out, v)
      }
      def open[From : TypeTag](ref : DataRef) = {
        ioTypeOf[From].open(ref)
      }
      def create[From](v:From, ref:DataCreatorRef)(implicit tag:TypeTag[From]) = {
        write(v, ref).open
      }
      def getIoTypeId(t:IoType[_]) = {
        Some(types.indexOf(t)).filter(_ >= 0)
      }
      def idIoType(i:Int) = types(i)

      def orderingOf[T](implicit ord: Ordering[T]) = ord

      def orderingOf(valueTypeTag: TypeTag[_]) = {
        (valueTypeTag.tpe match {
          case t if t == typeOf[Boolean] => orderingOf[Boolean]
          case t if t == typeOf[Int] => orderingOf[Int]
          case t if t == typeOf[Long] => orderingOf[Long]
          case t if t == typeOf[String] => orderingOf[String]
          case _ =>
            throw new IllegalArgumentException(valueTypeTag.tpe + " is unknown")
        }).asInstanceOf[Ordering[Any]] // TODO: refactor, there has to be a better way
      }
      def anyOrdering : Ordering[Any] = new Ordering[Any] {
        val b = orderingOf[Boolean]
        val i = orderingOf[Int]
        val l = orderingOf[Long]
        val s = orderingOf[String]
        override def compare(x: Any, y: Any): Int = {
          (x, y) match {
            case (_ : MinBound, _) => -1
            case (_ : MaxBound, _) => 1
            case (_, _ : MinBound) => 1
            case (_, _ : MaxBound) => -1
            case (xv: Boolean, yv:Boolean) => b.compare(xv, yv)
            case (xv: Int, yv:Int) => i.compare(xv, yv)
            case (xv: Long, yv:Long) => l.compare(xv, yv)
            case (xv: String, yv:String) => s.compare(xv, yv)
            case (x, y) =>
              throw new IllegalArgumentException("cannot compare " + x + " with " + y)
          }
        }
      }
      def refSeqType = seqTypeOf[IoObject]
    }
  }
  def strings = {
    val str = new strSeqType
    val buf = new ArrayBuffer[IoType[_]]()
    val self = apply(buf)
    val entryIo = new Serializer[(Int, String, Long)] {
      override def read(b: RandomAccess, pos: Long): (Int, String, Long) = {
        val i = b.getBeInt(pos)
        val l = b.getBeLong(pos+4)
        val s = StringIo.read(b, pos+12)
        (i, s, l)
      }
      override def write(o: DataOutputStream, v: (Int, String, Long)): Unit = {
        o.writeInt(v._1)
        o.writeLong(v._3)
        StringIo.write(o, v._2)
      }
      override def size(o: RandomAccess, pos: Long): Long = {
        4+8+StringIo.size(o, pos+4+8)
      }
    }
    val javaIo = new JavaObjectIo[Any]
    val variantIo = new VariantIo(Array(BooleanIo, IntIo, LongIo, StringIo), javaIo)
    val tupleIo = new Tuple2Io[String, Any](StringIo, variantIo)
    val bitsIoType =
      new IoBitsType[String](// converts Bits
        new SparseIoBitsType[String](),
        new DenseIoBitsType[String])
    buf ++=
      Seq(
        str,
        new IntIoArrayType[String],
        new LongIoArrayType[String],
        IoBitsType.booleanSeqIoType(bitsIoType),// new BooleanIoSeqType[String](),  // converts Seq[Boolean]
        bitsIoType,
        //        new SparseToDenseIoBitsType[String](),
        new IntIoArrayType[String],
        new RefIoSeqType[String, IoObject](self,
          new ObjectIoSeqType[String, (Int, String, Long)](entryIo, entryIo)),
        new ObjectIoSeqType[String, (String, Any)](tupleIo, tupleIo),
        new ObjectIoSeqType[String, Any](javaIo, javaIo))
    self
  }
}
