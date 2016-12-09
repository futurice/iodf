package com.futurice.iodf.ioseq

import java.io._

import com.futurice.iodf.store.{Dir, IoData, RandomAccess}
import com.futurice.iodf.Utils._
import com.futurice.iodf._
import xerial.larray.buffer.LBufferAPI

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

trait OutputWriting[T] {
  def write(o:DataOutputStream, v:T)
}

trait RandomAccessReading[T] {
  def read(o:RandomAccess, pos:Long) : T
  def size(o:RandomAccess, pos:Long) : Long
}

trait Serializer[T] extends OutputWriting[T] with RandomAccessReading[T] {

}
trait TypedSerializer[T] extends Serializer[T] {
  def clazz : Class[T]
  def tryWrite(o:DataOutputStream, t:Any) = write(o, t.asInstanceOf[T])
}

class ObjectIoSeqType[Id, T](i:RandomAccessReading[T], o:OutputWriting[T])(
  implicit val t: TypeTag[Seq[T]], vTag:TypeTag[T])
  extends IoTypeOf[Id, IoSeq[Id, T], Seq[T]]()(t)
  with SeqIoType[Id, IoSeq[Id, T], T] {

  override def write(output: DataOutputStream, v: Seq[T]): Unit = {
    val w = new ObjectIoSeqWriter[T](output, o)
    v.foreach { w.write(_) }
    w.writeIndex
  }

  override def open(buf: IoData[Id])= {
    new ObjectIoSeq[Id, T](IoRef(this, buf.ref), buf.randomAccess, i)
  }

  override def valueTypeTag = vTag
}


class ObjectIoSeqWriter[T](out:OutputStream, io:OutputWriting[T]) extends Closeable {
  val pos = new ArrayBuffer[Long]()
  val o = new DataOutputStream(out)
  def close = {
    o.flush
    o.close()
  }
  def write(v:T) : Unit = {
    pos += o.size
    io.write(o, v)
  }
  def writeIndex : Unit = {
    val indexPos = o.size
    pos.foreach { p =>
      o.writeLong(p)
    }
    o.writeLong(indexPos)
  }
}

class ObjectIoSeq[Id, T](val ref:IoRef[Id, _], buf:RandomAccess, i:RandomAccessReading[T]) extends IoSeq[Id, T] {
//  new RuntimeException().printStackTrace()

/*  System.out.println("io " + i.getClass.toString)
  System.out.println("data id " + ref.dataRef.id)
  System.out.println("index size " + buf.size)*/
  val indexPos = buf.getBeLong(buf.size - 8)
//  System.out.println("index pos " + indexPos)
  val lsize = ((buf.size-8)-indexPos) / 8
/*  System.out.println("size " + lsize)
  System.out.println("positions " + (0 until lsize.toInt).map(objectPos(_)))
  System.out.println("hashes: " + this.hashCode + "/" + buf.hashCode)
  System.out.println("addr: " + buf.address)*/

  def objectPos(l:Long) = buf.getBeLong(indexPos + l*8)
  override def apply(l: Long): T = {
/*    System.out.println("getting item " + l)
    System.out.println("hashes: " + this.hashCode + "/" + buf.hashCode)
    System.out.println("addr: " + buf.address)
    System.out.println("position: " + objectPos(l))*/
    i.read(buf, objectPos(l))
  }
  override def close(): Unit = {
//    System.out.println("object io seq closed")
    buf.close
  }
}

class JavaObjectIo[T] extends Serializer[T] {
  override def write(o: DataOutputStream, v: T) : Unit = {
    val buf = new ByteArrayOutputStream()
    using(new ObjectOutputStream(buf)) { oo =>
      oo.writeObject(v)
    }
    val bytes = buf.toByteArray
    o.writeInt(bytes.size)
    o.write(bytes)
  }
  override def read(o: RandomAccess, pos: Long): T = {
//    System.out.println("reading from pos " + pos)
    val size = o.getBeInt(pos)
    val buf = new Array[Byte](size)
    o.copyTo(pos+4, buf)
    using (new ObjectInputStream(new ByteArrayInputStream(buf))) { in =>
      in.readObject().asInstanceOf[T]
    }
  }
  override def size(o: RandomAccess, pos: Long): Long = {
    o.getBeInt(pos) + 4
  }
}

class VariantIo(tpes:Array[TypedSerializer[_]], fallback:Serializer[Any]) extends Serializer[Any] {
  val lookup =
    tpes.zipWithIndex.map(e => (e._1.clazz, (e._2, e._1))).toMap
      : Map[Class[_], (Int, TypedSerializer[_])]

  override def write(o: DataOutputStream, v: Any): Unit = {
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
  override def read(o: RandomAccess, pos: Long): Any = {
    tpe(o.getBeInt(pos)).read(o, pos+4)
  }
  override def size(o: RandomAccess, pos: Long): Long = {
    val t = tpes(o.getBeInt(pos))
    tpe(o.getBeInt(pos)).size(o,pos+4)
  }
}

class Tuple2Io[F, S](first:Serializer[F], second:Serializer[S]) extends Serializer[(F, S)] {
  override def write(o: DataOutputStream, v: (F, S)) : Unit = {
    first.write(o, v._1)
    second.write(o, v._2)
  }
  override def read(o: RandomAccess, pos: Long): (F, S) = {
    val f = first.read(o, pos)
    val fsz = first.size(o, pos)
    val s = second.read(o, pos + fsz)
    (f, s)
  }
  override def size(o: RandomAccess, pos: Long): Long = {
    val fsz = first.size(o, pos)
    fsz + second.size(o, pos + fsz)
  }
}
object BooleanIo extends TypedSerializer[Boolean] {
  override def read(o: RandomAccess, pos: Long): Boolean = {
    o.getByte(pos) != 0
  }
  override def size(o: RandomAccess, pos: Long): Long = {
    1
  }
  override def write(o: DataOutputStream, v: Boolean): Unit = {
    o.writeByte(if (v) 1 else 0)
  }
  override def clazz = classOf[Boolean]
}
object IntIo extends TypedSerializer[Int] {
  override def read(o: RandomAccess, pos: Long): Int = {
    o.getBeInt(pos)
  }
  override def size(o: RandomAccess, pos: Long): Long = {
    4
  }
  override def write(o: DataOutputStream, v: Int): Unit = {
    o.writeInt(v)
  }
  override def clazz = classOf[Int]
}

object StringIo extends TypedSerializer[String] {
  override def read(o: RandomAccess, pos: Long): String = {
    val sz = o.getBeInt(pos)
//    System.out.println("sz " + sz + " at pos " + pos)
    val buf = new Array[Byte](sz)
    o.copyTo(pos+4, buf)
    new String(buf)
  }

  override def write(o: DataOutputStream, v: String): Unit = {
    val bytes = v.getBytes
    o.writeInt(bytes.size)
    o.write(bytes)
  }
  override def size(o: RandomAccess, pos: Long): Long = {
    o.getBeInt(pos) + 4
  }
  override def clazz = classOf[String]
}

class StringIoSeqType[Id](implicit t:TypeTag[Seq[String]], vTag:TypeTag[String])
  extends ObjectIoSeqType[Id, String](StringIo, StringIo)(t, vTag) {
}