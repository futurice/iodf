package com.futurice.iodf.io

import java.io.OutputStream

import com.futurice.iodf.util.{Handle, Ref, Tracing}
import xerial.larray.buffer.{Memory, UnsafeUtil}

/**
  * Random Access to some data.
  *
  * This is now a rather raw access operating now the memory. The rawness of the access
  * is there to guarantee good performance, as this class can become easily the performance bottle
  *
  * Some day, we could possibly abstract this, and provide access to some remote resources
  * using the same interface? (maybe by operating on pages)
  */
class DataAccess(val _dataRef:DataRef,
                 val _memRef:Ref[Memory],
                 val from:Long = 0,
                 val until:Option[Long] = None)
  extends Handle {

  val dataRef = _dataRef.openCopy
  val ref = _memRef.openCopy
  Tracing.opened(this)

  override def close(): Unit = {
    dataRef.close()
    ref.close
    Tracing.closed(this)
  }

  override def openCopy = new DataAccess(dataRef, ref, from, until)

  val m = ref.get

  val address = m.address() + from
  val size    = until.getOrElse(m.dataSize()) - from

  val unsafe = UnsafeUtil.getUnsafe

  if (address < m.address() || address + size > m.address() + m.size())
    throw new RuntimeException(f"slice [$from, ${from+size}] ouf of bounds [0, ${m.size}]")

  def unsafeGetMemoryByte(memory:Long) = {
    unsafe.getByte(memory)
  }

  def safeGetMemoryByte(memory:Long) = {
    if (ref.isClosed) {
      throw new RuntimeException("closed")
    }
    if (memory < address || memory >= address + size) {
      throw new RuntimeException(memory + s" is outside the range [$address, ${address+size}]")
    }
    try {
      unsafe.getByte(memory)
    } catch {
      case e : Throwable =>
        System.out.println("exception for " + address + " of size " + size + " when accessing " + memory + ", block:" + m.address() + " sz: " +m.size())
        throw e
    }
  }

  def getMemoryByte(memory:Long) = {
    //safeGetMemoryByte(memory)
    unsafeGetMemoryByte(memory)
  }

  def checkRange(offset:Long, sz:Long) = {
    if (offset < 0 || offset + sz > size) {
      throw new RuntimeException(offset + s" is outside the range [0, $size]")
    }
    if (ref.isClosed) {
      throw new RuntimeException("memory resource " + m.address + " is closed")
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
  def unsafeGetBeLongByAddress(address:Long) = {
    ((getMemoryByte(address) : Long) << (8*7)) +
      ((getMemoryByte(address + 1) & 0xFF : Long) << (8*6)) +
      ((getMemoryByte(address + 2) & 0xFF : Long) << (8*5)) +
      ((getMemoryByte(address + 3) & 0xFF : Long) << (8*4)) +
      ((getMemoryByte(address + 4) & 0xFF : Long) << (8*3)) +
      ((getMemoryByte(address + 5) & 0xFF : Long) << (8*2)) +
      ((getMemoryByte(address + 6) & 0xFF : Long) << (8*1)) +
      (getMemoryByte(address + 7) & 0xFF : Long)
  }

  // big endian long
  def getBeLong(offset:Long) = {
    checkRange(offset, 8)
    val m = address + offset
    unsafeGetBeLongByAddress(m)
  }
  def unsafeGetLeLongByAddress(address:Long) = {
    ((getMemoryByte(address) & 0xFF: Long)) +
      ((getMemoryByte(address + 1) & 0xFF : Long) << (8*1)) +
      ((getMemoryByte(address + 2) & 0xFF : Long) << (8*2)) +
      ((getMemoryByte(address + 3) & 0xFF : Long) << (8*3)) +
      ((getMemoryByte(address + 4) & 0xFF : Long) << (8*4)) +
      ((getMemoryByte(address + 5) & 0xFF : Long) << (8*5)) +
      ((getMemoryByte(address + 6) & 0xFF : Long) << (8*6)) +
      ((getMemoryByte(address + 7) & 0xFF : Long) << (8*7))
  }
  // little endian long
  def getLeLong(offset:Long) = {
    checkRange(offset, 8)
    unsafeGetLeLongByAddress(address + offset)
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

  def openView(from:Long, until:Long) =
    new DataAccess(dataRef, ref, this.from + from, Some(this.from + until))
}