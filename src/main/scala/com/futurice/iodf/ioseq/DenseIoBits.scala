package com.futurice.iodf.ioseq

import java.io.DataOutputStream

import com.futurice.iodf.store.{Dir, FileRef, IoData, RandomAccess}
import xerial.larray.buffer.LBufferAPI
import com.futurice.iodf._

import scala.reflect.runtime.universe._

object DenseIoBitsType {
  def bitsToLongCount(bitSize:Long) = ((bitSize+63L) / 64L)
  def bitsToByteSize(bitSize:Long) = bitsToLongCount(bitSize) * 8
}

class DenseIoBitsType[Id](implicit val t:TypeTag[Seq[Boolean]])
  extends IoTypeOf[Id, DenseIoBits[Id], Seq[Boolean]]()(t)
    with SeqIoType[Id, DenseIoBits[Id], Boolean] {

/*  def create(id:Id, data:Seq[Boolean], dir:Dir[Id]) = {
    val buf = dir.create(id, 8 + DenseIoBitsType.bitsToByteSize(data.size))
    buf.putLong(0, data.size)
    val rv =
      new DenseIoBits[Id](IoRef(this, dir, id), buf)
    data.zipWithIndex.foreach { case (d, i) =>
      rv.update(i, d)
    }
    rv
  }
  def open(id:Id, dir:Dir[Id], pos:Long) = {
    new DenseIoBits[Id](IoRef(this, dir, id, pos), dir.open(id, pos))
  }*/
  /*  def tryCreate[I](id:Id, data:I, dir:Dir[Id]) = {
      Some( create(id, data.asInstanceOf[In], dir) )
    }*/
  override def write(output: DataOutputStream, v: Seq[Boolean]): Unit = {
    output.writeLong(v.size)
    var i = 0
    while (i < v.size) {
      var l = 0.toByte
      var mask = 1.toByte
      val max = Math.min(i+8, v.size)
      while (i < max) {
        if (v(i)) l = (l | mask).toByte
        mask = (mask << 1).toByte
        i += 1
      }
      output.writeByte(l)
    }
  }

  override def open(buf: IoData[Id]): DenseIoBits[Id] = {
    new DenseIoBits[Id](IoRef(this, buf.ref), buf.randomAccess)
  }

  override def valueTypeTag =
    _root_.scala.reflect.runtime.universe.typeTag[Boolean]
}

/**
  * Created by arau on 24.11.2016.
  */
class DenseIoBits[Id](val ref:IoRef[Id, DenseIoBits[Id]], val origBuf:RandomAccess) extends IoSeq[Id, Boolean] with java.io.Closeable {

  val bitSize = origBuf.getBeLong(0)

  val buf = origBuf.openSlice(8)

  def close = { origBuf.close; buf.close } //close both handles

  val longCount = DenseIoBitsType.bitsToLongCount(bitSize)

  def byteSize = buf.size

  def lsize = bitSize
  def offset = 8

  def long(l:Long) : Long = buf.getNativeLong(l*8)
  def putLong(index:Long, l:Long) = buf.putNativeLong(index*8, l)

  def longs = for (pos <- (0L until longCount)) yield long(pos)

  def bitCount = {
    var rv = 0L
    longs.foreach { l =>
      rv += java.lang.Long.bitCount(l)
    }
    rv
  }

  def :=(bits:DenseIoBits[Id]) = {
    if (bitSize != bits.bitSize) {
      throw new IllegalArgumentException("given bitset is of different length")
    }
    (0L until longCount).foreach { i =>
      putLong(i, bits.long(i))
    }
  }

  def &=(bits:DenseIoBits[Id]) = {
    if (bitSize != bits.bitSize) {
      throw new IllegalArgumentException("given bitset is of different length")
    }
    (0L until longCount).foreach { i =>
      putLong(i, long(i) & bits.long(i))
    }
  }

  def andCount(bits:DenseIoBits[Id]) = {
    if (bitSize != bits.bitSize) {
      throw new IllegalArgumentException("given bitset is of different length")
    }
    var rv = 0L
    (0L until longCount).foreach { i =>
      rv += java.lang.Long.bitCount(
        long(i) & bits.long(i)
      )
    }
    rv
  }

  def apply(i:Long) : Boolean = {
    val addr = i / 8L
    val offset = i % 8
    ((buf.getByte(addr) >> offset) & 1) == 1
  }

  def update(i:Long, v:Boolean) = {
    val addr = i / 8L
    val offset = i % 8
    if (v) {
      buf.putByte(addr, (buf.getByte(addr) | (1<<offset)).toByte)
    } else {
      buf.putByte(addr, (buf.getByte(addr) & (~(1<<offset))).toByte)
    }
  }

  override def toString = {
    val rv = new StringBuffer()
    (0L until bitSize).foreach { i =>
      rv.append(if (apply(i)) "1" else "0")
    }
    rv.toString
  }
}

