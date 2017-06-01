package com.futurice.iodf

import java.io.{BufferedOutputStream, DataOutputStream}

import com.futurice.iodf.Utils.using
import com.futurice.iodf.ioseq._
import com.futurice.iodf.store.{DataRef, FileRef, RandomAccess}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._


trait IoTypes[IoId] {
  def orderingOf(valueTypeTag: TypeTag[_]) : Ordering[Any]

  def ioTypeOf(t : Type) : IoTypeOf[IoId, _ <: IoObject[IoId], _]
  def ioTypeOf[T : TypeTag]() : IoTypeOf[IoId, _ <: IoObject[IoId], T]
  def writeIoObject[From](t:From, ref:FileRef[IoId])(implicit tag:TypeTag[From]) : IoRef[IoId, _ <: IoObject[IoId]]
  def createIoObject[From](t:From, ref:FileRef[IoId])(implicit tag:TypeTag[From]) : IoObject[IoId]
  def openIoObject[From](ref:FileRef[IoId])(implicit tag:TypeTag[From]) : IoObject[IoId]

  def getIoTypeId(t:IoType[IoId, _]) : Option[Int]
  def ioTypeId(t:IoType[IoId, _]) = {
    getIoTypeId(t) match {
      case Some(id) => id
      case None => throw new RuntimeException("type " + t + " of ClassLoader " + t.getClass.getClassLoader + " not found for type scheme " + this + " of ClassLoader " + IoTypes.this.getClass.getClassLoader)
    }
  }
  def idIoType(i:Int) : IoType[IoId, _]

  def intToId(i:Int) : IoId

  // shortcuts: reconsider
  def idOrdering : Ordering[IoId]
  def anyOrdering : Ordering[Any]
  def idSeqType : SeqIoType[IoId, _ <: IoSeq[IoId, IoId], IoId]
  def longSeqType: SeqIoType[IoId, _ <: IoSeq[IoId, Long], Long]

}


object IoTypes {
  type strSeqType = StringIoSeqType[String]

  def apply[Id](types:Seq[IoType[Id, _ <: IoObject[Id]]],
                _idIo:Serializer[Id],
                _intToId: Int => Id)(implicit idTag:TypeTag[Id], idOrd: Ordering[Id]) = {
    new IoTypes[Id] {
      def ioTypeOf(t:Type) : IoTypeOf[Id, _ <: IoObject[Id], _] = {
        types.find(_.asTypeOf(t).isDefined).map(_.asTypeOf(t).get) match {
          case Some(v) => v
          case None =>
            throw new RuntimeException("no io type for " + t + " / " + t.hashCode() + ". ")
        }
      }
      def ioTypeOf[T : TypeTag]() : IoTypeOf[Id, _ <: IoObject[Id], T] = {
        ioTypeOf(typeOf[T]).asInstanceOf[IoTypeOf[Id, _ <: IoObject[Id], T]]
      }
      def writeIoObject[From : TypeTag](v:From, ref:FileRef[Id]) = {
        val typ = ioTypeOf[From]
        using(new DataOutputStream(new BufferedOutputStream(ref.openOutput))) {
          typ.write(_, v)
        }
        new IoRef[Id, IoObject[Id]](typ, new DataRef[Id](ref.dir, ref.id))
      }
      def openIoObject[From](ref:FileRef[Id])(implicit tag:TypeTag[From]) = {
        using (ref.open) { ioTypeOf[From].open(_) }
      }
      def createIoObject[From](v:From, ref:FileRef[Id])(implicit tag:TypeTag[From]) = {
        writeIoObject(v, ref)
        openIoObject(ref)(tag)
      }
      def getIoTypeId(t:IoType[Id, _]) = {
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
      def idOrdering = idOrd
      def anyOrdering : Ordering[Any] = new Ordering[Any] {
        val b = orderingOf[Boolean]
        val i = orderingOf[Int]
        val l = orderingOf[Long]
        val s = orderingOf[String]
        override def compare(x: Any, y: Any): Int = {
          (x, y) match {
            case (xv: Boolean, yv:Boolean) => b.compare(xv, yv)
            case (xv: Int, yv:Int) => i.compare(xv, yv)
            case (xv: Long, yv:Long) => l.compare(xv, yv)
            case (xv: String, yv:String) => s.compare(xv, yv)
          }
        }
      }
      def idSeqType : SeqIoType[Id, _ <: IoSeq[Id, Id], Id] = {
        ioTypeOf[Seq[Id]].asInstanceOf[SeqIoType[Id, _ <: IoSeq[Id, Id], Id]]
      }
      def longSeqType : SeqIoType[Id, _ <: IoSeq[Id, Long], Long] = {
        ioTypeOf[Seq[Long]].asInstanceOf[SeqIoType[Id, _ <: IoSeq[Id, Long], Long]]
      }

      override def intToId(i: Int): Id = _intToId(i)
    }
  }
  def strings = {
    val str = new strSeqType
    val buf = new ArrayBuffer[IoType[String, _ <: IoObject[String]]]()
    val self = apply(buf, StringIo, "_" + _.toString)
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
    buf ++=
      Seq(
        str,
        new IntIoArrayType[String],
        new LongIoArrayType[String],
        new BooleanIoSeqType[String](),  // converts Seq[Boolean]
        new IoBitsType[String](// converts Bits
          new SparseIoBitsType[String](),
          new DenseIoBitsType[String]()),
        //        new SparseToDenseIoBitsType[String](),
        new IntIoArrayType[String],
        new RefIoSeqType[String,
          IoSeq[String, IoObject[String]]](self,
          new ObjectIoSeqType[String, (Int, String, Long)](entryIo, entryIo)),
        new ObjectIoSeqType[String, (String, Any)](tupleIo, tupleIo),
        new ObjectIoSeqType[String, Any](javaIo, javaIo))
    self
  }
}
