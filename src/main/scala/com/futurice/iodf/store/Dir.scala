package com.futurice.iodf.store

import java.io.{Closeable, InputStream, OutputStream}
import java.util.logging.{Level, Logger}

import xerial.larray.buffer.{LBufferAPI, Memory, UnsafeUtil}

import scala.collection.mutable

object RefCounted {
  val l = Logger.getLogger("RefCounted")
  var openRefs = 0
  var traces : Option[mutable.Map[RefCounted[_], Exception]] = None


  def opened(ref:RefCounted[_ <: java.io.Closeable]) = {
 //   l.log(Level.INFO, "open objects " + open + " -> " + (open+i))
    traces match {
      case Some(tr) => tr += (ref -> new RuntimeException("leaked reference"))
      case None =>
    }
    new mutable.HashMap[RefCounted[_], RuntimeException]
    openRefs += 1
  }

  def closed(ref:RefCounted[_]) = {
    traces match {
      case Some(tr) =>
        tr.remove(ref)
      case None =>
    }
    openRefs -= 1
  }

  def trace[T](f : => T) : T = {
    // FIXME: not thread safe! Use threadlocal!
    val tracesBefore = traces
    traces = Some(new mutable.HashMap[RefCounted[_], Exception]())
    try {
      f
    } finally {
      if (traces.get.size > 0) {
        l.log(Level.SEVERE, traces.get.size + " references leaked.")
        traces.get.take(1).map { t =>
          l.log(Level.SEVERE, t._1.v + " leaked!", t._2)
        }
      }
      traces = tracesBefore
    }
  }
}


case class RefCounted[V <: Closeable](val v:V, var count:Int = 0) extends Closeable {
  RefCounted.opened(this)
  def open = {
//    System.out.println("inc " + RefCounted.this.hashCode())
//    new RuntimeException().printStackTrace()
    count += 1
    v
  }
  override def hashCode(): Int = v.hashCode()
  def close = {
//    System.out.println("dec " + RefCounted.this.hashCode())
//    new RuntimeException().printStackTrace()
    count -= 1
    if (count == 0) {
      RefCounted.closed(this)
      v.close
    }
  }

}

case class MemoryResource(memory:Memory, resource:Closeable) extends Closeable {
  def close = resource.close
}

class RandomAccess(countedM:RefCounted[MemoryResource], offset:Long = 0) extends Closeable {
  val m = countedM.open.memory
  override def close(): Unit = countedM.close

  val address = m.address() + offset
  val size = m.dataSize() - offset

  def unsafe = UnsafeUtil.getUnsafe

  def getByte(offset:Long) = unsafe.getByte(address + offset)
  def putByte(offset:Long, byte:Byte) = unsafe.putByte(address + offset, byte)

  def getNativeLong(offset:Long) = unsafe.getLong(address + offset)
  def putNativeLong(offset:Long, value:Long) = unsafe.putLong(address + offset, value)

  def copyTo(srcOffset:Long, bytes:Array[Byte], destOffset:Int, n:Int) : Unit = {
    (0 until n).foreach { i =>
      bytes(destOffset + i) = getByte(srcOffset + i)
    }
  }
  def copyTo(srcOffset:Long, bytes:Array[Byte]) : Unit =
    copyTo(srcOffset, bytes, 0, bytes.size)

  // big endian long
  def getBeLong(offset:Long) = {
    val m = address + offset
    ((unsafe.getByte(m) : Long) << (8*7)) +
    ((unsafe.getByte(m + 1) & 0xFF : Long) << (8*6)) +
    ((unsafe.getByte(m + 2) & 0xFF : Long) << (8*5)) +
    ((unsafe.getByte(m + 3) & 0xFF : Long) << (8*4)) +
    ((unsafe.getByte(m + 4) & 0xFF : Long) << (8*3)) +
    ((unsafe.getByte(m + 5) & 0xFF : Long) << (8*2)) +
    ((unsafe.getByte(m + 6) & 0xFF : Long) << (8*1)) +
    (unsafe.getByte(m + 7) & 0xFF : Long)
  }

  def getBeInt(offset:Long) = {
    val m = address + offset
    ((unsafe.getByte(m) & 0xFF : Int) << (8*3)) +
    ((unsafe.getByte(m + 1) & 0xFF : Int) << (8*2)) +
    ((unsafe.getByte(m + 2) & 0xFF: Int) << (8*1)) +
    (unsafe.getByte(m + 3) & 0xFF: Int)
  }


  def getBeShort(offset:Long) = {
    val m = address + offset
      ((unsafe.getByte(m) & 0xFF).toShort << (8*1)) +
      (unsafe.getByte(m + 1) & 0xFF).toShort
  }

  def openSlice(offset:Long) = new RandomAccess(countedM, this.offset + offset)

}
trait IoData[Id] extends Closeable {
  def ref : DataRef[Id]

//  def inputStream(pos:Long = 0L) : InputStream
  def randomAccess : RandomAccess
  def size : Long

/*  def openView(offset:Long) : IoData[Id]
  def openView(offset:Long, size:Long) : IoData[Id]*/
}

object IoData {
  def apply[Id](r:DataRef[Id], mem: RefCounted[MemoryResource]) = {
    new IoData[Id] {
      val m = mem.open.memory
      override def close(): Unit = mem.close
      override def ref: DataRef[Id] = {
        r
      }
      override def randomAccess = new RandomAccess(mem)

      override def size: Long = m.size()

/*      override def openView(offset: Long): IoData[Id] = ???

      override def openView(offset: Long, size: Long): IoData[Id] = ???*/

    }
  }
}

/**
  * Created by arau on 24.11.2016.
  */
trait Dir[Id] extends Closeable {

  // select i:th id in order
  def id(i:Int) : Id

//  def create(id:Id, length:Long) : IoData[Id]
  def output(id:Id) : OutputStream

  def open(id:Id, pos:Long = 0) : IoData[Id]

  def list : Array[Id]

  def ref(id:Id) = new FileRef(this, id)
  def ref(id:Id, pos:Long) = new DataRef(this, id, pos)
}
case class FileRef[Id](dir:Dir[Id], id:Id) {
  def open = dir.open(id)
  def output = dir.output(id)
//  def create(l:Long) = dir.create(id, l)
}

case class DataRef[Id](dir:Dir[Id], id:Id, pos:Long = 0) {
  def open = dir.open(id, pos)
}
