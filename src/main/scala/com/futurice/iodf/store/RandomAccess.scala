package com.futurice.iodf.store

import xerial.larray.buffer.{LBufferAPI, Memory, UnsafeUtil}

import java.io.{Closeable, OutputStream}

case class MemoryResource(memory:Memory, resource:Closeable) extends Closeable {
  /*  val l = Logger.getLogger("MemoryResource")
    l.info(memory.address() + " opened:\n" + new RuntimeException().getStackTrace.mkString("\n"))*/
  var isClosed = false
  def close = {
    //    l.info(memory.address() + " closed")
    isClosed = true
    resource.close
  }
}

class RandomAccess(val countedM:RefCounted[MemoryResource],
                   val from:Long = 0,
                   val until:Option[Long] = None)
  extends Closeable {
  val m = countedM.open.memory
  override def close(): Unit = countedM.close

  val address = m.address() + from
  val size    = until.getOrElse(m.dataSize())

  val unsafe = UnsafeUtil.getUnsafe

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
      case e : Throwable =>
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
    if (countedM.count <= 0 || countedM.value.isClosed) {
      throw new RuntimeException("memory resource " + countedM.value.memory.address + " is closed")
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

  def openView(from:Long, until:Long) = new RandomAccess(countedM, this.from + from, Some(this.from + until))
}