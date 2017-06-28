package com.futurice.iodf.io

import java.io.{DataOutput, OutputStream}

trait LDataOutputStream extends OutputStream with DataOutput {
  // this is why we need separate output stream :-(
  // the ints will overflow
  def pos : Long
}

object LDataOutputStream {
  def apply(out:OutputStream, pos:Long) = {
    def p = pos
    new LDataOutputStream {

      var pos: Long = p

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

      override def writeFloat(v: Float): Unit = {

      }

      override def writeLong(v: Long): Unit = {
        var shift = 54
        while (shift >= 0) {
          write((v >>> shift).toByte)
          shift -= 8
        }
      }

      override def writeDouble(v: Double): Unit = {

      }

      override def writeInt(v: Int): Unit = {
        var shift = 24
        while (shift >= 0) {
          write((v >>> shift).toByte)
          shift -= 8
        }
      }

      override def writeByte(v: Int): Unit = {
        write(v)
      }

      override def writeChar(v: Int): Unit = {
        write((v >>> 8).toByte)
        write(v.toByte)
      }

      def writeString(l:String) : Unit = {
        writeVInt(l.length)
        (0 until l.length).foreach { c =>
          writeVInt(l.charAt(c))
        }
      }
      def writeShort(l:Int) : Unit = {
        write((l >>> 8).toByte)
        write(l.toByte)
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

      override def writeBytes(s: String): Unit = ???

      override def writeUTF(s: String): Unit = ???

      override def writeChars(s: String): Unit = ???

      override def writeBoolean(v: Boolean): Unit = ???
    }

  }

}
