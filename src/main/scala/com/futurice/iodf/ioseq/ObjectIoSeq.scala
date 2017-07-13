package com.futurice.iodf.ioseq

import java.io._

import com.futurice.iodf._
import com.futurice.iodf.io.{DataAccess, DataOutput, DataRef, IoRef}
import com.futurice.iodf.util.{LSeq, MultiSeq, Ref}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

trait OutputWriting[T] {
  def write(o:DataOutput, v:T)
}

trait RandomAccessReading[T] {
  def read(o:DataAccess, pos:Long) : T
  def size(o:DataAccess, pos:Long) : Long
}

trait Serializer[T] extends OutputWriting[T] with RandomAccessReading[T] {

}
trait TypedSerializer[T] extends Serializer[T] {
  def clazz : Class[T]
  def tryWrite(o:DataOutput, t:Any) = write(o, t.asInstanceOf[T])
}

class ObjectIoSeqType[T](i:RandomAccessReading[T], o:OutputWriting[T])(
  implicit ifaceTag: TypeTag[LSeq[T]], vTag:TypeTag[T])
  extends SeqIoType[T, LSeq[T], ObjectIoSeq[T]] {

  override def interfaceType: universe.Type = ifaceTag.tpe
  override def ioInstanceType: universe.Type = typeOf[ObjectIoSeq[T]]

  override def write(output: DataOutput, v: LSeq[T]): Unit = {
    val w = new ObjectIoSeqWriter[T](output, o)
    v.foreach { w.write(_) }
    w.writeIndex
  }
  override def writeMerged(out: DataOutput, seqs: Seq[Ref[LSeq[T]]]): Unit = {
    seqs.exists(!_.isInstanceOf[ObjectIoSeq[T]]) match {
      case true => // there is at least one sequence, which cannot be white box merged
        super.writeMerged(out, seqs) // go generic
      case false => // go whitebox merge: awe can skip serialization & deserialization
        ObjectIoSeqWriter.writeMerged(out, seqs.map(_.asInstanceOf[ObjectIoSeq[T]]))
    }
  }

  override def open(buf: DataAccess) = {
    new ObjectIoSeq[T](new IoRef(this, buf.dataRef), buf, i)
  }

  override def valueTypeTag = vTag

  override def viewMerged(seqs: Seq[Ref[LSeq[T]]]): LSeq[T] =
    new MultiSeq[T, LSeq[T]](seqs.toArray)

  override def toString =
    f"ObjectIoSeqType[${vTag.tpe}]"

}


class ObjectIoSeqWriter[T](out:DataOutput, io:OutputWriting[T]) extends Closeable {
  val pos = new ArrayBuffer[Long]()
  var at = 0L
  def close = {
    out.flush
    out.close()
  }
  def write(v:T) : Unit = {
    pos += at
    val before = out.pos
    io.write(out, v)
    at += (out.pos - before)
  }
  def writeIndex : Unit = {
    val indexPos = at
    pos.foreach { p =>
      out.writeLong(p)
    }
    out.writeLong(indexPos)
  }
}

object ObjectIoSeqWriter {
  def writeMerged(out: DataOutput, seqs: Seq[ObjectIoSeq[_]]): Unit = {

    // 1. write copy data areas, avoiding deserialization & serialization
    var at = 0L
    val offsets =
      seqs.map { seq =>
        var begin = at
        seq.buf.writeTo(0, out, seq.indexPos)
        at += seq.indexPos
        begin
      }
    val indexPos = at

    // 2. then, write indexes
    (offsets zip seqs).foreach { case (offset, seq) =>
      (0L until seq.lsize).foreach { i =>
        out.writeLong(offset + seq.objectPos(i))
      }
    }
    // 3. last, write the index position
    out.writeLong(indexPos)
    out.close
  }
}

class ObjectIoSeq[T](_openRef:IoRef[ObjectIoSeq[T]],
                     val _buf:DataAccess,
                     val i:RandomAccessReading[T]) extends IoSeq[T] {

  def openRef = _openRef.openCopy

  override def close(): Unit = {
    _openRef.close
    buf.close
  }
  val buf = _buf.openCopy

  val indexPos = buf.getBeLong(buf.size - 8)
  val lsize = ((buf.size-8)-indexPos) / 8

  def objectPos(l:Long) = {
    val rv = buf.getBeLong(indexPos + l*8)
    if (rv < 0 || rv >= indexPos) throw new RuntimeException(f"object data position $rv out of data area [0, $indexPos]")
    rv
  }
  override def apply(l: Long): T = {
    if (l >= 0 && l < lsize) {
      i.read(buf, objectPos(l))
    } else {
      throw new ArrayIndexOutOfBoundsException(f"index $l is out of range [0, $lsize]")
    }
  }

}

class JavaObjectIo[T] extends Serializer[T] {
  override def write(o: DataOutput, v: T) : Unit = {
    val buf = new ByteArrayOutputStream()
    using(new ObjectOutputStream(buf)) { oo =>
      oo.writeObject(v)
    }
    val bytes = buf.toByteArray
    o.writeInt(bytes.size)
    o.write(bytes)
  }
  override def read(o: DataAccess, pos: Long): T = {
//    System.out.println("reading from pos " + pos)
    val size = o.getBeInt(pos)
    val buf = new Array[Byte](size)
    o.copyTo(pos+4, buf)

/*    using (new ObjectInputStream(new ByteArrayInputStream(buf))) { in =>
      in.readObject().asInstanceOf[T]
    }*/

    val loader = Thread.currentThread().getContextClassLoader
    using (new ObjectInputStream(new ByteArrayInputStream(buf)) {
      @throws[IOException]
      @throws[ClassNotFoundException]
      protected override def resolveClass(desc: ObjectStreamClass): Class[_] = {
        val name: String = desc.getName
        Class.forName(name, false, loader)
      }
    }) { in =>
      in.readObject().asInstanceOf[T]
    }
  }
  override def size(o: DataAccess, pos: Long): Long = {
    o.getBeInt(pos) + 4
  }
}

class VariantIo(tpes:Array[TypedSerializer[_]], fallback:Serializer[Any]) extends Serializer[Any] {
  val lookup =
    tpes.zipWithIndex.map(e => (e._1.clazz, (e._2, e._1))).toMap
      : Map[Class[_], (Int, TypedSerializer[_])]

  override def write(o: DataOutput, v: Any): Unit = {
    lookup.get(v.getClass) match {
      case Some(e) =>
        o.write(e._1)
        e._2.tryWrite(o, v)
      case None =>
        o.write(-1)
        fallback.write(o, v)
    }
  }
  def tpe(i:Int) = {
    i match {
      case -1 => fallback
      case v => tpes(v)
    }
  }
  override def read(o: DataAccess, pos: Long): Any = {
    tpe(o.getByte(pos)).read(o, pos+1)
  }
  override def size(o: DataAccess, pos: Long): Long = {
    val t = tpes(o.getByte(pos))
    tpe(o.getBeInt(pos)).size(o,pos+1)
  }
}

class Tuple2Io[F, S](first:Serializer[F], second:Serializer[S]) extends Serializer[(F, S)] {
  override def write(o: DataOutput, v: (F, S)) : Unit = {
    first.write(o, v._1)
    second.write(o, v._2)
  }
  override def read(o: DataAccess, pos: Long): (F, S) = {
    val f = first.read(o, pos)
    val fsz = first.size(o, pos)
    val s = second.read(o, pos + fsz)
    (f, s)
  }
  override def size(o: DataAccess, pos: Long): Long = {
    val fsz = first.size(o, pos)
    fsz + second.size(o, pos + fsz)
  }
}
object BooleanIo extends TypedSerializer[Boolean] {
  override def read(o: DataAccess, pos: Long): Boolean = {
    o.getByte(pos) != 0
  }
  override def size(o: DataAccess, pos: Long): Long = {
    1
  }
  override def write(o: DataOutput, v: Boolean): Unit = {
    o.writeByte(if (v) 1 else 0)
  }
  override def clazz = classOf[Boolean]
}
object IntIo extends TypedSerializer[Int] {
  override def read(o: DataAccess, pos: Long): Int = {
    o.getBeInt(pos)
  }
  override def size(o: DataAccess, pos: Long): Long = {
    4
  }
  override def write(o: DataOutput, v: Int): Unit = {
    o.writeInt(v)
  }
  override def clazz = classOf[Int]
}
object LongIo extends TypedSerializer[Long] {
  override def read(o: DataAccess, pos: Long): Long = {
    o.getBeLong(pos)
  }
  override def size(o: DataAccess, pos: Long): Long = {
    8
  }
  override def write(o: DataOutput, v: Long): Unit = {
    o.writeLong(v)
  }
  override def clazz = classOf[Long]
}

object StringIo extends TypedSerializer[String] {
  override def read(o: DataAccess, pos: Long): String = {
    val sz = o.getBeInt(pos)
//    System.out.println("sz " + sz + " at pos " + pos)
    val buf = new Array[Byte](sz)
    o.copyTo(pos+4, buf)
    new String(buf)
  }

  override def write(o: DataOutput, v: String): Unit = {
    val bytes = v.getBytes
    o.writeInt(bytes.size)
    o.write(bytes)
  }
  override def size(o: DataAccess, pos: Long): Long = {
    o.getBeInt(pos) + 4
  }
  override def clazz = classOf[String]
}

class StringIoSeqType(implicit ifaceTag:TypeTag[LSeq[String]],
                      vTag:TypeTag[String])
  extends ObjectIoSeqType[String](StringIo, StringIo)(ifaceTag, vTag) {

}