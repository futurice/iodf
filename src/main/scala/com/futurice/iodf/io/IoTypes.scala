package com.futurice.iodf.io

import java.io.{BufferedOutputStream, DataOutputStream, OutputStream}

import com.futurice.iodf.Utils.using
import com.futurice.iodf.ioseq._
import com.futurice.iodf.store.{AllocateOnce, DataRef, RandomAccess}
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
  def orderingOf(valueTypeTag: Type) : Ordering[Any]


/*  def ioType = new IoType[Any] {
    override def valueType: universe.Type = typeOf[Any]
    override def open(data: DataRef): Any =
      open[]()
    override def write(out: DataOutputStream, iface: Any): Unit = ???
  }*/

/*  def writerEntryOf(t:Type) : (IoWriter[_], Int)
  def openerEntryOf(t:Type) : (IoOpener[_], Int)

  def writerOf(t:Type) = writerEntryOf(t)._1
  def openerOf(t:Type) = openerEntryOf(t)._1*/

  def ioTypeOf(t : Type)      : IoType[_, _]
  def ioTypeOf[T : TypeTag]() : IoType[_ >: T, _ <: T]

  def write(out:DataOutputStream, t:Any, tpe:Type) : Unit

  def write[Interface](out:DataOutputStream, t:Interface)(
    implicit tag:TypeTag[Interface]) : Unit

  def create[Interface](ref:AllocateOnce, t:Interface)(
    implicit tag:TypeTag[Interface]) : Interface

  def open(ref:DataRef) : Any

  def openAs[Interface](ref:DataRef) = open(ref).asInstanceOf[Interface]

//  def ioTypeOf(ref:DataRef) : (IoType[_, _], Int)

  def write[Interface](out:OutputStream, t:Interface)(
    implicit tag:TypeTag[Interface]): Unit =  {
    write[Interface](new DataOutputStream(out), t)
  }

  /** note: this returns and opened a data reference */
  def written[Interface](ref:AllocateOnce, t:Interface)(
    implicit tag:TypeTag[Interface]): DataRef = {
    using (ref.create) { out =>
      write(new DataOutputStream(out), t)
      out.adoptResult
    }
  }


  def getIoTypeId(t:IoType[_, _]) : Option[Int]

  def ioTypeId(t:IoType[_, _]) = {
    getIoTypeId(t) match {
      case Some(id) => id
      case None => throw new RuntimeException("type " + t + " of ClassLoader " + t.getClass.getClassLoader + " not found for type scheme " + this + " of ClassLoader " + IoTypes.this.getClass.getClassLoader)
    }
  }
  def idIoType(i:Int) : IoType[_, _]

  // shortcuts: reconsider
//  def idOrdering : Ordering[IoId]
  def anyOrdering : Ordering[Any]

  def intToId[Id](implicit tag:TypeTag[Id]) : Int => Id

  val TypeRef(seqPkg, seqSymbol, anyArgs) = typeOf[LSeq[Any]]
  def toLSeqType(tpe:Type) =
    scala.reflect.runtime.universe.internal.typeRef(
      seqPkg, seqSymbol, List(tpe))

  def seqTypeOf[Member](implicit typeTag:TypeTag[LSeq[Member]])
    : IoSeqType[Member, LSeq[Member],_ <: IoSeq[Member]] = {
    ioTypeOf[LSeq[Member]].asInstanceOf[IoSeqType[Member, LSeq[Member],_ <: IoSeq[Member]]]
  }
  def lseqTypeOf(implicit memberTpe:Type)
  : IoSeqType[_, LSeq[_],_ <: IoSeq[_]] = {
    ioTypeOf(toLSeqType(memberTpe)).asInstanceOf[IoSeqType[_, LSeq[_],_ <: IoSeq[_]]]
  }

  def longLSeqType = seqTypeOf[Long]
//  def idSeqType : IoSeqType[IoId, LSeq[IoId],_ <: IoSeq[IoId, IoId]]
//  def refSeqType : IoSeqType[IoObject, LSeq[IoObject],_ <: IoSeq[IoObject]]
  def bitsLSeqType : IoSeqType[Boolean, LBits,_ <: IoSeq[Boolean]] = {
    ioTypeOf[LBits].asInstanceOf[IoSeqType[Boolean, LBits,_ <: IoSeq[Boolean]]]
  }
}

abstract class Bounds() {}
case class MinBound() extends Bounds() {}
case class MaxBound() extends Bounds() {}

class IoTypesImpl(types:Seq[IoType[_, _]]) extends IoTypes {
  def writerEntryOf(t:Type) : (IoWriter[_], Int) = {
    types.toStream
      .zipWithIndex
      .flatMap(e => e._1.castToWriter(t).map(c => (c, e._2)))
      .headOption match {
      case Some(v) => v
      case None =>
        throw new RuntimeException("no io type for " + t + " / " + t.hashCode() + ". ")
    }
  }
  def openerEntryOf(t:Type) : (IoOpener[_], Int) = {
    types.toStream
      .zipWithIndex
      .flatMap(e => e._1.castToOpener(t).map(c => (c, e._2)))
      .headOption match {
      case Some(v) => v
      case None =>
        throw new RuntimeException("no io type for " + t + " / " + t.hashCode() + ". ")
    }
  }
  def openerEntryOf[T : TypeTag] : (IoType[_ >: T, _ <: T], Int) = {
    openerEntryOf(typeOf[T]).asInstanceOf[(IoType[_ >: T, _ <: T], Int)]
  }
  def writerEntryOf[T : TypeTag] : (IoType[T, _ <: T], Int) = {
    writerEntryOf(typeOf[T]).asInstanceOf[(IoType[T, _ <: T], Int)]
  }
  override def ioTypeOf(t:Type) : IoType[_, _] = {
    types(openerEntryOf(t)._2)
  }
  override def ioTypeOf[T : TypeTag]() : IoType[_ >: T, _ <: T] = {
    types(openerEntryOf(typeOf[T])._2).asInstanceOf[IoType[_ >: T, _ <: T]]
  }
  override def write(out:DataOutputStream, v:Any, tpe:Type) = {
    val (typ, id) = writerEntryOf(tpe)
    out.writeInt(id)
    typ.asAnyWriter.write(out, v)
  }
  override def write[From : TypeTag](out:DataOutputStream, v:From) = {
    write(out, v, typeOf[From])
  }
  override def open(ref : DataRef) = {
//    val (typ, id) = openerEntryOf[From]
    using (ref.open) { m =>
      val id = m.getBeInt(0)
      using (ref.openView(4, ref.byteSize)) { view =>
        types(id).open(view)
      }
    }
  }
  override def create[From](ref:AllocateOnce, v:From)(implicit tag:TypeTag[From]) = {
    val (typ, id) = writerEntryOf[From]
    using(
      using(ref.create) { out =>
        typ.write(new DataOutputStream(out), v)
        out.adoptResult
      }) { typ.open }
  }
  override def getIoTypeId(t:IoType[_, _]) = {
    Some(types.indexOf(t)).filter(_ >= 0)
  }
  override def idIoType(i:Int) = types(i)

  def orderingOf[T](implicit ord: Ordering[T]) = ord

  override def orderingOf(tpe: Type) = {
    (tpe match {
      case t if t == typeOf[Boolean] => orderingOf[Boolean]
      case t if t == typeOf[Int] => orderingOf[Int]
      case t if t == typeOf[Long] => orderingOf[Long]
      case t if t == typeOf[String] => orderingOf[String]
      case v =>
        throw new IllegalArgumentException(v + " is unknown")
    }).asInstanceOf[Ordering[Any]] // TODO: refactor, there has to be a better way
  }
  override def anyOrdering : Ordering[Any] = new Ordering[Any] {
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
  override def intToId[Id](implicit tag:TypeTag[Id]) =
    tag.tpe match {
      case t if t == typeOf[Int] => (i => i.asInstanceOf[Id])
      case t if t == typeOf[String] => (i => i.toString.asInstanceOf[Id])

    }

}

object IoTypes {

  def apply(types:Seq[IoType[_, _]]) = {
    new IoTypesImpl(types)
  }
  def strings = {
    val str = new StringIoSeqType
    val buf = new ArrayBuffer[IoType[_, _]]()
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
      new IoBitsType(// converts Bits
        new SparseIoBitsType(),
        new DenseIoBitsType())
    buf ++=
      Seq(
        str,
        new IntIoArrayType,
        new LongIoArrayType,
        IoBitsType.booleanSeqIoType(bitsIoType),// new BooleanIoSeqType[String](),  // converts Seq[Boolean]
        bitsIoType,
        //        new SparseToDenseIoBitsType[String](),
        new IntIoArrayType,
/*        new RefIoSeqType[IoObject](self,
          new ObjectIoSeqType[(Int, String, Long)](entryIo, entryIo)),*/
        new ObjectIoSeqType[(String, Any)](tupleIo, tupleIo),
        new ObjectIoSeqType[Any](javaIo, javaIo))
    self
  }
}
