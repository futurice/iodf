package com.futurice.iodf.io

import java.io.OutputStream

class LDataOutputStream(out:OutputStream, var pos:Long = 0L) extends OutputStream {
  def writeVInt(l:Int) : Unit = {
    var w = l
    while (w >= 128) {
      write( 0x80 | (w & 0x7f) )
      w = w >>> 7
    }
    write(w)
    w = 0
  }
  def writeVLong(l:Long) : Unit = {
    var w = l
    while (w >= 128) {
      write( 0x80 | (w & 0x7f).toByte )
      w = w >>> 7
    }
    write(w.toByte)
    w = 0
  }
  def writeString(l:String) : Unit = {
    writeVInt(l.length)
    (0 until l.length).foreach { c =>
      writeVInt(l.charAt(c))
    }
  }
  def writeLeShort(l:Short) : Unit = {
    write((l >>> 8).toByte)
    write(l.toByte)
  }
  def writeLeInt(l:Int) : Unit = {
    var shift = 24
    while (shift >= 0) {
      write((l >>> shift).toByte)
      shift -= 8
    }
  }
  def writeLeLong(l:Long) : Unit = {
    var shift = 54
    while (shift >= 0) {
      write((l >>> shift).toByte)
      shift -= 8
    }
  }
  override def write(b: Array[Byte], off:Int, len:Int): Unit = {
    out.write(b, off, len)
    pos += len
  }
  override def write(b: Array[Byte]): Unit = {
    out.write(b)
    pos += b.size
  }
  override def write(b: Int): Unit = {
    out.write(b)
    pos += 1
  }
  override def flush = out.flush
  override def close = out.close
}
