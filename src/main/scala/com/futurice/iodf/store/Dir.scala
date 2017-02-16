package com.futurice.iodf.store

import java.io.{Closeable, InputStream, OutputStream}
import java.util.logging.{Level, Logger}

import xerial.larray.buffer.{LBufferAPI, Memory, UnsafeUtil}

import scala.collection.mutable

object RefCounted {
  val l = Logger.getLogger("RefCounted")
  var openRefs = 0
  var traces : Option[mutable.Map[RefCounted[_], Exception]] = None


  def opened(ref:RefCounted[_ <: java.io.Closeable]) = synchronized {
 //   l.log(Level.INFO, "open objects " + open + " -> " + (open+i))
    traces match {
      case Some(tr) => tr += (ref -> new RuntimeException("leaked reference"))
      case None =>
    }
    new mutable.HashMap[RefCounted[_], RuntimeException]
    openRefs += 1
  }

  def closed(ref:RefCounted[_]) = synchronized {
    traces match {
      case Some(tr) =>
        tr.remove(ref)
      case None =>
    }
    openRefs -= 1
  }

  def trace[T](f : => T) : T = synchronized {
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
  def open = synchronized  {
//    System.out.println("inc " + RefCounted.this.hashCode())
//    new RuntimeException().printStackTrace()
    count += 1
    v
  }
  override def hashCode(): Int = v.hashCode()
  def close = synchronized {
//    System.out.println("dc " + RefCounted.this.hashCode())
//    new RuntimeException().printStackTrace()
    count -= 1
    if (count == 0) {
      RefCounted.closed(this)
      v.close
    }
  }

}

case class MemoryResource(memory:Memory, resource:Closeable) extends Closeable {
  def close = {
//    System.out.println(memory.address() + " closed")
    resource.close
  }
}

class RandomAccess(val countedM:RefCounted[MemoryResource],
                   val offset:Long = 0,
                   val maxSize:Option[Long] = None)
    extends Closeable {
  val m = countedM.open.memory
  override def close(): Unit = countedM.close

  val address = m.address() + offset
  val size = maxSize.getOrElse(m.dataSize() - offset)

  def unsafe = UnsafeUtil.getUnsafe

  if (address < m.address() || address + size > m.address() + m.size())
    throw new RuntimeException("slice out of bounds")

  def unsafeGetMemoryByte(memory:Long) = {
    unsafe.getByte(memory)
  }

  def safeGetMemoryByte(memory:Long) = {
    if (countedM.count <= 0) {
      throw new RuntimeException("closed")
    }
    if (memory < address || memory >= address + size) {
      throw new RuntimeException(memory + s" is outside the range [$address, ${address+size}]")
    }
    try {
      unsafe.getByte(memory)
    } catch {
      case e =>
        System.out.println("exception for " + address + " of size " + size + " when accessing " + memory + ", block:" + m.address() + " sz: " +m.size())
        throw e
    }
  }

  def getMemoryByte(memory:Long) = {
//    safeGetMemoryByte(memory)
    unsafeGetMemoryByte(memory)
  }

  def checkRange(offset:Long, sz:Long) = {
    if (offset < 0 || offset + sz > size) {
      throw new RuntimeException(offset + s" is outside the range [0, $size]")
    }
  }

  def getByte(offset:Long) = {
    checkRange(offset, 1)
    unsafe.getByte(address + offset)
  }
  def putByte(offset:Long, byte:Byte) = {
    checkRange(offset, 1)
    unsafe.putByte(address + offset, byte)
  }

  def getNativeLong(offset:Long) = {
    checkRange(offset, 8)
    unsafe.getLong(address + offset)
  }
  def putNativeLong(offset:Long, value:Long) = {
    checkRange(offset, 8)
    unsafe.putLong(address + offset, value)
  }

  def copyTo(srcOffset:Long, bytes:Array[Byte], destOffset:Int, n:Int) : Unit = {
    checkRange(srcOffset, n)
    (0 until n).foreach { i =>
      bytes(destOffset + i) = getByte(srcOffset + i)
    }
  }
  def copyTo(srcOffset:Long, bytes:Array[Byte]) : Unit =
    copyTo(srcOffset, bytes, 0, bytes.size)

  def writeTo(srcOffset:Long, out:OutputStream, n:Long) : Unit = {
    checkRange(srcOffset, n)
    var i = srcOffset+address
    val end = srcOffset+address+n
    val bufsz = 8*1024
    val buf = new Array[Byte](bufsz)
    while (i < end) {
      val bend = Math.min(end, i + bufsz)
      var w = 0
      while (i < bend) {
        buf(w) = unsafe.getByte(i)
        i += 1
        w +=1
      }
      out.write(buf, 0, w)
    }
  }

  // big endian long
  def getBeLong(offset:Long) = {
    checkRange(offset, 8)
    val m = address + offset
    ((getMemoryByte(m) : Long) << (8*7)) +
    ((getMemoryByte(m + 1) & 0xFF : Long) << (8*6)) +
    ((getMemoryByte(m + 2) & 0xFF : Long) << (8*5)) +
    ((getMemoryByte(m + 3) & 0xFF : Long) << (8*4)) +
    ((getMemoryByte(m + 4) & 0xFF : Long) << (8*3)) +
    ((getMemoryByte(m + 5) & 0xFF : Long) << (8*2)) +
    ((getMemoryByte(m + 6) & 0xFF : Long) << (8*1)) +
    (getMemoryByte(m + 7) & 0xFF : Long)
  }
  // little endian long
  def getLeLong(offset:Long) = {
    checkRange(offset, 8)
    val m = address + offset
    ((getMemoryByte(m) & 0xFF: Long)) +
     ((getMemoryByte(m + 1) & 0xFF : Long) << (8*1)) +
     ((getMemoryByte(m + 2) & 0xFF : Long) << (8*2)) +
     ((getMemoryByte(m + 3) & 0xFF : Long) << (8*3)) +
     ((getMemoryByte(m + 4) & 0xFF : Long) << (8*4)) +
     ((getMemoryByte(m + 5) & 0xFF : Long) << (8*5)) +
     ((getMemoryByte(m + 6) & 0xFF : Long) << (8*6)) +
     ((getMemoryByte(m + 7) & 0xFF : Long) << (8*7))
  }

  def getBeInt(offset:Long) = {
    checkRange(offset, 4)
    val m = address + offset
    ((getMemoryByte(m) & 0xFF : Int) << (8*3)) +
    ((getMemoryByte(m + 1) & 0xFF : Int) << (8*2)) +
    ((getMemoryByte(m + 2) & 0xFF: Int) << (8*1)) +
    (getMemoryByte(m + 3) & 0xFF: Int)
  }


  def getBeShort(offset:Long) = {
    checkRange(offset, 2)
    val m = address + offset
      ((getMemoryByte(m) & 0xFF).toShort << (8*1)) +
      (getMemoryByte(m + 1) & 0xFF).toShort
  }

  def openSlice(offset:Long) = new RandomAccess(countedM, this.offset + offset, maxSize.map(_ - offset))
  def openSlice(offset:Long, size:Long) = new RandomAccess(countedM, this.offset + offset, Some(size))

}
trait IoData[Id] extends Closeable {
  def ref : DataRef[Id]

//  def inputStream(pos:Long = 0L) : InputStream
  def openRandomAccess : RandomAccess
  def size : Long

  def openView(offset:Long, length:Option[Long] = None) : IoData[Id]
}

object IoData {
  def open[Id](r:DataRef[Id], mem: RefCounted[MemoryResource]) : IoData[Id] = {
    new IoData[Id] {
      val m = mem.open.memory
      override def close(): Unit = mem.close
      override def ref: DataRef[Id] = {
        r
      }
      override def openRandomAccess = new RandomAccess(mem, r.pos, r.size)

      override def size: Long = m.size()

      override def openView(offset: Long, size:Option[Long]): IoData[Id] = {
        open(r.view(offset, size), mem)
      }
    }
  }
}

/**
  * Created by arau on 24.11.2016.
  */
trait Dir[Id] extends Closeable {

  // select i:th id in order
  def id(i:Int) : Id

  def freeId : Id = {
    def find(i:Int) : Id = {
      val rv = id(i)
      if (exists(rv)) {
        find(i+1)
      } else {
        rv
      }
    }
    find(0)
  }

//  def create(id:Id, length:Long) : IoData[Id]
  def openOutput(id:Id) : OutputStream

  def open(id:Id, pos:Long = 0, size:Option[Long] = None) : IoData[Id]

  def list : Array[Id]
  def exists(id:Id) = list.contains(id)

  def ref(id:Id) = new FileRef(this, id)
  def ref(id:Id, pos:Long, size:Option[Long]) = new DataRef(this, id, pos, size)
}

case class FileRef[Id](dir:Dir[Id], id:Id) {
  def open = dir.open(id)
  def openOutput = dir.openOutput(id)
  def toDataRef = new DataRef[Id](dir, id, 0, None)
//  def create(l:Long) = dir.create(id, l)
}
/* TODO: add maxSize:Option[Long] */
case class DataRef[Id](dir:Dir[Id], id:Id, pos:Long = 0, size:Option[Long] = None) {
  def open = dir.open(id, pos, size)

  def view(offset:Long, sz:Option[Long] = None) = {
    new DataRef(dir, id,
                pos + offset,
                sz.orElse(size.map(_ - offset)))
  }
}
