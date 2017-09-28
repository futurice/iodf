package com.futurice.iodf.io

import com.futurice.iodf.{IoContext, IoScope, Utils}
import com.futurice.iodf._
import com.futurice.iodf.df._
import com.futurice.iodf.ioseq._
import com.futurice.iodf.store.{AllocateOnce, RamAllocator}
import com.futurice.iodf.util.{KeyMap, LBits, LSeq, Ref}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._


/**
 * TODO: refactor some day.. this should be split into parts
 *
 * FIXME: this is crappy design. This class should have one goal: provide open/write types.
 */

trait IoTypes {

  // TODO: refactor following ->


  // TODO: <-

  //

  def ioTypeOf[T : TypeTag]() : IoType[_ >: T, _ <: T]

  def ioTypeOf(t : Type)      : IoType[_, _]
  def ioTypeOf(data:DataAccess)       : IoType[_, _]

  def write(out:DataOutput, t:Any, tpe:Type) : Unit

  def write[Interface](out:DataOutput, t:Interface)(
    implicit tag:TypeTag[Interface]) : Unit

  def create[Interface](ref:AllocateOnce, t:Interface)(
    implicit tag:TypeTag[Interface]) : Interface

  def open(ref:DataAccess) : Any
  def open(ref:DataRef) : Any = {
    using (ref.openAccess)(open)
  }

  def openAs[Interface](ref:DataAccess) =
    open(ref).asInstanceOf[Interface]
  def openAs[Interface](ref:DataRef) =
    open(ref).asInstanceOf[Interface]

  /** note: this returns and opened a data reference */
  def openSave[Interface](ref:AllocateOnce, t:Interface)(
    implicit tag:TypeTag[Interface]): DataRef = {
    using (ref.create) { out =>
      write(out, t)
      out.openDataRef
    }
  }
  def save[Interface:TypeTag](ref:AllocateOnce, t:Interface)(
    implicit bind:IoScope): DataRef = {
    bind(openSave(ref, t))
  }

  def openSave(ref:AllocateOnce, t:Any, typ:Type) = {
    using (ref.create) { out =>
      write(out, t, typ)
      out.openDataRef
    }
  }
  def save(ref:AllocateOnce, t:Any, typ:Type)(
    implicit bind:IoScope): DataRef = {
    bind(openSave(ref, t, typ))
  }
  def getIoTypeId(t:IoType[_, _]) : Option[Int]

  def ioTypeId(t:IoType[_, _]) = {
    getIoTypeId(t) match {
      case Some(id) => id
      case None => throw new RuntimeException("type " + t + " of ClassLoader " + t.getClass.getClassLoader + " not found for type scheme " + this + " of ClassLoader " + IoTypes.this.getClass.getClassLoader)
    }
  }
  def typeId(tpe:Type) =
    ioTypeId(ioTypeOf(tpe))
  def idIoType(i:Int) : IoType[_, _]

  // shortcuts: reconsider
//  def idOrdering : Ordering[IoId]

  val TypeRef(seqPkg, seqSymbol, anyArgs) = typeOf[LSeq[Any]]
  def toLSeqType(tpe:Type) =
    scala.reflect.runtime.universe.internal.typeRef(
      seqPkg, seqSymbol, List(tpe))

  def seqTypeOf[Member](implicit typeTag:TypeTag[LSeq[Member]])
    : SeqIoType[Member, LSeq[Member],_ <: IoSeq[Member]] = {
    ioTypeOf[LSeq[Member]].asInstanceOf[SeqIoType[Member, LSeq[Member],_ <: IoSeq[Member]]]
  }
  def seqTypeOf(implicit memberTpe:Type)
  : SeqIoType[_, LSeq[_],_ <: IoSeq[_]] = {
    ioTypeOf(toLSeqType(memberTpe)).asInstanceOf[SeqIoType[_, LSeq[_],_ <: IoSeq[_]]]
  }

  def longLSeqType = seqTypeOf[Long]
//  def idSeqType : IoSeqType[IoId, LSeq[IoId],_ <: IoSeq[IoId, IoId]]
//  def refSeqType : IoSeqType[IoObject, LSeq[IoObject],_ <: IoSeq[IoObject]]
  def bitsLSeqType : SeqIoType[Boolean, LBits,_ <: IoSeq[Boolean]] = {
    ioTypeOf[LBits].asInstanceOf[SeqIoType[Boolean, LBits,_ <: IoSeq[Boolean]]]
  }

  def +(tpe:IoType[_, _]) : IoTypes
}

abstract class Bounds() {}
case class MinBound() extends Bounds() {}
case class MaxBound() extends Bounds() {}

class IoTypesImpl(types:Seq[IoType[_, _]]) extends IoTypes {
  lazy val reversedWithIndex =
    types.zipWithIndex.reverse

  def +(tpe:IoType[_, _]) = new IoTypesImpl(types ++ Seq[IoType[_, _]](tpe))

  def findWriterEntryOf(t:Type) : (IoWriter[_], Int) = {
    reversedWithIndex
      .toStream
      .flatMap(e => e._1.provideWriter(t).map(c => (c, e._2)))
      .headOption match {
      case Some(v) => v
      case None =>
        throw new RuntimeException("no io type for " + t + " / " + t.hashCode() + ". ")
    }
  }
  def findOpenerEntryOf(t:Type) : (IoOpener[_], Int) = {
    reversedWithIndex
      .toStream
      .flatMap(e => e._1.provideOpener(t).map(c => (c, e._2)))
      .headOption match {
      case Some(v) => v
      case None =>
        throw new RuntimeException("no io type for " + t + " / " + t.hashCode() + ". ")
    }
  }
  val writerEntryLookup = mutable.HashMap[Type, (IoWriter[_], Int)]()
  val openerEntryLookup = mutable.HashMap[Type, (IoOpener[_], Int)]()

  def writerEntryOf(t:Type) : (IoWriter[_], Int) = {
    writerEntryLookup.getOrElseUpdate(t,
      findWriterEntryOf(t))
  }
  def openerEntryOf(t:Type) : (IoOpener[_], Int) = {
    openerEntryLookup.getOrElseUpdate(t,
      findOpenerEntryOf(t))
  }

  def openerEntryOf[T : TypeTag] : (IoType[_ >: T, _ <: T], Int) = {
    openerEntryOf(typeOf[T]).asInstanceOf[(IoType[_ >: T, _ <: T], Int)]
  }
  def writerEntryOf[T : TypeTag] : (IoType[T, _ <: T], Int) = {
    writerEntryOf(typeOf[T]).asInstanceOf[(IoType[T, _ <: T], Int)]
  }
  override def ioTypeOf(t:Type) : IoType[_, _] = {
    types(writerEntryOf(t)._2)
  }
  override def ioTypeOf[T : TypeTag]() : IoType[_ >: T, _ <: T] = {
    types(writerEntryOf(typeOf[T])._2).asInstanceOf[IoType[_ >: T, _ <: T]]
  }
  override def write(out:DataOutput, v:Any, tpe:Type) = {
    val (typ, id) = writerEntryOf(tpe)
    out.writeInt(id)
    typ.asAnyWriter.write(out, v)
  }
  override def write[From : TypeTag](out:DataOutput, v:From) = {
    write(out, v, typeOf[From])
  }
  override def ioTypeOf(data:DataAccess) = {
    types(data.getBeInt(0))
  }
  override def open(data : DataAccess) = {
//    val (typ, id) = openerEntryOf[From]
    val id = data.getBeInt(0)
    using (data.openView(4, data.size)) { view =>
      types(id).open(view)
    }
  }
  override def create[From](ref:AllocateOnce, v:From)(implicit tag:TypeTag[From]) = {
    val (typ, id) = writerEntryOf[From]
    using(
      using(ref.create) { out =>
        typ.write(out, v)
        out.openDataRef
      }) { typ.open }
  }
  override def getIoTypeId(t:IoType[_, _]) = {
    Some(types.indexOf(t)).filter(_ >= 0)
  }
  override def idIoType(i:Int) = types(i)

}

object IoTypes {

  def apply(types:Seq[IoType[_, _]]) = {
    new IoTypesImpl(types)
  }
  val (default : IoTypes, stringDfType : ColsIoType[String], indexType : IndexIoType[String]) = {
    scoped { implicit bind =>
      val strSeq = new StringIoSeqType
      val buf = new ArrayBuffer[IoType[_, _]]()
      val self = apply(buf)
      implicit val io =
        bind(new IoContext(self, bind(Ref.open(new RamAllocator()))))

      val entryIo = new Serializer[(Int, String, Long)] {
        override def read(b: DataAccess, pos: Long): (Int, String, Long) = {
          val i = b.getBeInt(pos)
          val l = b.getBeLong(pos + 4)
          val s = StringIo.read(b, pos + 12)
          (i, s, l)
        }

        override def write(o: DataOutput, v: (Int, String, Long)): Unit = {
          o.writeInt(v._1)
          o.writeLong(v._3)
          StringIo.write(o, v._2)
        }

        override def size(o: DataAccess, pos: Long): Long = {
          4 + 8 + StringIo.size(o, pos + 4 + 8)
        }
      }
      val javaIo = new JavaObjectIo[Any]
      val rootIo = new IoVar[Any](None)
      val keyMapIo = new KeyMapIo(StringIo, rootIo)
      val variantIo =
        new VariantIo(
          Array(BooleanIo,
                IntIo,
                LongIo,
                StringIo,
                keyMapIo,
                new OptionIo[Any](rootIo),
                new Tuple2Io[Any, Any](rootIo, rootIo),
                new ArrayIo[Any](rootIo)),
          javaIo)
      rootIo.io = Some(variantIo)

      val tupleIo = new Tuple2Io[String, Any](StringIo, variantIo)
      val stringIntIo = new Tuple2Io[String, Int](StringIo, IntIo)

      val bitsIoType =
        new BitsIoType(// converts Bits
          new SparseIoBitsType(),
          new DenseIoBitsType())

      val longSeq = new LongIoArrayType

      implicit val stringValueOrdering = Index.indexColIdOrdering[String]
      implicit val intValueOrdering = Index.indexColIdOrdering[Int]
      implicit val longValueOrdering = Index.indexColIdOrdering[Long]

      val stringSchema = new ColSchemaIoType[String]()
      val stringValueSchema = new ColSchemaIoType[(String, Any)]()

      val cols = new ColIoType()
      val stringDfs = new ColsIoType[String]()

      val stringValueDfs = new ColsIoType[(String, Any)]()

      val indexDfs      = new IndexIoType(stringValueDfs)
      val tables        = new TableIoType
      val indexedTables = new IndexedIoType(tables, indexDfs)
      val documents =     new DocumentsIoType(stringDfs)
      val indexedDocuments = new IndexedIoType(documents, indexDfs)

      val intSeq = new IntIoArrayType
      val boolSeq = BitsIoType.booleanSeqIoType(bitsIoType)

      val longArrayIo = new ArrayIo[Long](LongIo)
      val anyArrayIo = new ArrayIo[Any](rootIo)
      val anyToAnyIo = new Tuple2Io[Any, Any](rootIo, rootIo)
      val strToAnyIo = new Tuple2Io[String, Any](StringIo, rootIo)
      val intToAnyIo = new Tuple2Io[Int, Any](IntIo, rootIo)

      // for map like behavior
      val anyToAnySeq = new ObjectIoSeqType[(Any, Any)](anyToAnyIo, anyToAnyIo)
      val strToAnySeq = new ObjectIoSeqType[(String, Any)](strToAnyIo, strToAnyIo)
      val intToAnySeq = new ObjectIoSeqType[(Int, Any)](intToAnyIo, intToAnyIo)

      // statistics look ups
      val longArraySeq = new ObjectIoSeqType[Array[Long]](longArrayIo, longArrayIo)
      val anyArraySeq = new ObjectIoSeqType[Array[Any]](anyArrayIo, anyArrayIo)
      val anySeq = new ObjectIoSeqType[Any](javaIo, javaIo)

      // Needed for bit rows
      val bitsSeq = new IoObjectIoSeqType[LBits](bitsIoType, longSeq)

      buf ++=
        Seq(
          ValueIoType(variantIo), // any value, backed up by Java serialization

          ValueIoType(BooleanIo),
          ValueIoType(IntIo),
          ValueIoType(LongIo),
          ValueIoType(StringIo),

          anySeq, // sequence of any value
          anyArraySeq,
          longArraySeq,

          // map lik
          anyToAnySeq,
          strToAnySeq,
          intToAnySeq,

          OptionIoSeqType(anySeq, longSeq), // sequence of any optional value
          new ObjectIoSeqType[(String, Any)](tupleIo, tupleIo),
          new ObjectIoSeqType[(String, Int)](stringIntIo, stringIntIo),
          new ObjectIoSeqType[KeyMap](keyMapIo, keyMapIo),
          strSeq,
          intSeq,
          longSeq,
          boolSeq,
          bitsIoType, // prefer lbits type over seq[boolean] type

          OptionIoSeqType(boolSeq, longSeq),
          OptionIoSeqType(intSeq, longSeq),
          OptionIoSeqType(longSeq, longSeq),
          OptionIoSeqType(strSeq, longSeq),

          stringSchema,
          stringValueSchema,
          new TableSchemaIoType(),

          stringDfs,
          new ColsIoType[Int](),
          new ColsIoType[Long](),

          stringValueDfs,
          new ColsIoType[(Int, Any)](),
          new ColsIoType[(Long, Any)](),

          bitsSeq,

          cols,

          indexDfs,
          tables,
          indexedTables,
          documents,
          indexedDocuments,

          indexDfs)

      (self, stringDfs, indexDfs)
    }
  }
}
