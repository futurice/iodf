package com.futurice.iodf.io

import java.io.{DataOutputStream, OutputStream}

/**
  * Created by arau on 5.7.2017.
  */
trait DataOutputMixin extends OutputStream with java.io.DataOutput {

  val o = new DataOutputStream(this)

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
  def writeLeLong(long:Long) = {
    o.writeByte((long & 0xFF).toInt)
    o.writeByte((long >>> (8 * 1)).toInt & 0xFF)
    o.writeByte((long >>> (8 * 2)).toInt & 0xFF)
    o.writeByte((long >>> (8 * 3)).toInt & 0xFF)
    o.writeByte((long >>> (8 * 4)).toInt & 0xFF)
    o.writeByte((long >>> (8 * 5)).toInt & 0xFF)
    o.writeByte((long >>> (8 * 6)).toInt & 0xFF)
    o.writeByte((long >>> (8 * 7)).toInt & 0xFF)
  }


  def writeBoolean(v: Boolean) = {
    o.writeBoolean(v)
  }

  def writeByte(v: Int) = {
    o.writeByte(v)
  }

  def writeShort(v: Int) = {
    o.writeShort(v)
  }

  def writeChar(v: Int) = {
    o.writeChar(v)
  }

  def writeInt(v: Int): Unit = {
    o.writeInt(v)
  }

  def writeLong(v: Long): Unit = {
    o.writeLong(v)
  }

  def writeFloat(v: Float): Unit = {
    o.writeFloat(v)
  }

  def writeDouble(v: Double) = {
    o.writeDouble(v)
  }

  def writeBytes(s: String): Unit ={
    o.writeBytes(s)
  }

  def writeChars(s: String): Unit = {
    o.writeChars(s)
  }

  def writeUTF(s: String): Unit = {
    o.writeUTF(s)
  }

}
