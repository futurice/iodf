package com.futurice.iodf.ioseq

import java.io.DataOutputStream
import java.util

import com.futurice.iodf.store.{Dir, FileRef, IoData, RandomAccess}
import xerial.larray.buffer.LBufferAPI
import com.futurice.iodf.{IoRef, _}

import scala.reflect.runtime.universe._


object DenseIoBits {
  def bitsToLongCount(bitSize:Long) = ((bitSize+63L) / 64L)
  def bitsToByteSize(bitSize:Long) = bitsToLongCount(bitSize) * 8

  def create[IoId](bits:Seq[Boolean])(implicit io:IoContext[IoId]) = {
    io.bits.createDense(io.dir, bits)
  }
  def apply[IoId](bits:Seq[Boolean])(implicit io:IoContext[IoId], scope:IoScope) = {
    scope.bind(create(bits))
  }
/*  def apply[IoId](bits:Seq[Boolean])(implicit io:IoContext[IoId], scope:IoScope) = {
    scope.bind(open(bits))
  }*/
  def writeLeLong(out:DataOutputStream, long:Long) = {
    out.writeByte((long & 0xFF).toInt)
    out.writeByte((long >>> (8*1)).toInt & 0xFF)
    out.writeByte((long >>> (8*2)).toInt & 0xFF)
    out.writeByte((long >>> (8*3)).toInt & 0xFF)
    out.writeByte((long >>> (8*4)).toInt & 0xFF)
    out.writeByte((long >>> (8*5)).toInt & 0xFF)
    out.writeByte((long >>> (8*6)).toInt & 0xFF)
    out.writeByte((long >>> (8*7)).toInt & 0xFF)
  }

  def write(output: DataOutputStream, bitN:Int, leLongs:Iterable[Long]): Unit = {
    output.writeLong(bitN)
    for (l <- leLongs) {
      writeLeLong( output,  l )
    }
  }
  def writeMerged(out: DataOutputStream,
                  aSize:Long,
                  as:Iterator[Long],
                  bSize:Long,
                  bs:Iterator[Long]): Unit = {
    out.writeLong(aSize + bSize)
    val alongs = (aSize / 64)
    (0L until alongs).foreach { i => writeLeLong(out, as.next) }
    val overbits = (aSize % 64).toInt
    var bLongCount = bitsToLongCount(bSize)
    if (overbits > 0) {
      var overflow = as.next
      var i = 0
      while (i < bLongCount) {
        val v = bs.next
        val w = (v << overbits) | overflow
        writeLeLong(out, w)
        overflow = v >>> (64 - overbits)
        i += 1
      }
      if (bitsToLongCount(aSize + bSize) > alongs + bLongCount) {
        writeLeLong(out, overflow)
      }
    } else {
      (0L until bLongCount).foreach { i => writeLeLong(out, bs.next) }
    }
  }
  def writeMerged(out: DataOutputStream, a:DenseIoBits[_], b:DenseIoBits[_]): Unit = {
    writeMerged(out, a.lsize, a.leLongs.iterator, b.lsize, b.leLongs.iterator)
  }
  def defaultSeq[Id](lsize:Long) = {
    new EmptyIoBits[Id](lsize)
  }
  def writeAnyMerged[Id](out: DataOutputStream, seqA:Any, seqB:Any): Unit = {
    (seqA, seqB) match {
      case (a:DenseIoBits[_], b:DenseIoBits[_]) =>
        writeMerged(out, a, b)
      case (a:DenseIoBits[_], b:EmptyIoBits[_]) =>
        writeMerged(out, a.lsize, a.leLongs.iterator, b.lsize, Stream.continually(0L).iterator)
      case (a:EmptyIoBits[_], b:DenseIoBits[_]) =>
        writeMerged(out, a.lsize, Stream.continually(0L).iterator, b.lsize, b.leLongs.iterator)
      case (a:DenseIoBits[_], b:SparseIoBits[_]) =>
        writeMerged(out, a.lsize, a.leLongs.iterator, b.lsize, b.leLongs)
    }
  }
}

// TODO: there are three different mappings from different data structures
//       to dense bits. These should be merged !!!

class SparseToDenseIoBitsType[Id](implicit val t:TypeTag[SparseBits])
  extends IoTypeOf[Id, DenseIoBits[Id], SparseBits]()(t)
    with SeqIoType[Id, DenseIoBits[Id], Boolean] {

  override def write(output: DataOutputStream, v: SparseBits): Unit = {
    output.writeLong(v.size)
    var i = 0
    val bits = new util.BitSet(v.size.toInt)
    v.trues.foreach { l => bits.set(l.toInt) }
    val bytes = bits.toByteArray
    for (b <- bytes) {
      output.writeByte(b)
    }
    // write trailing bytes
    val byteSize = DenseIoBits.bitsToByteSize(v.size)
    for (i <- bytes.size until byteSize.toInt) {
      output.writeByte(0)
    }
  }
  override def writeMerged(output: DataOutputStream, a:DenseIoBits[Id], b:DenseIoBits[Id]) =
    DenseIoBits.writeMerged(output, a, b)
  override def writeAnyMerged(output: DataOutputStream, a:Any, b:Any) =
    DenseIoBits.writeAnyMerged(output, a, b)
  override def defaultSeq(lsize: Long): Any = DenseIoBits.defaultSeq(lsize)

  override def open(buf: IoData[Id]) = {
    new DenseIoBits[Id](IoRef(this, buf.ref), buf.openRandomAccess)
  }
  override def valueTypeTag =
    _root_.scala.reflect.runtime.universe.typeTag[Boolean]
}

class DenseIoBitsType[Id](implicit val t:TypeTag[Seq[Boolean]])
  extends IoTypeOf[Id, DenseIoBits[Id], Seq[Boolean]]()(t)
    with SeqIoType[Id, DenseIoBits[Id], Boolean] {

  def write(output: DataOutputStream, bitN:Int, v:Iterable[Long]) =
    DenseIoBits.write(output, bitN, v)

  override def write(output: DataOutputStream, v:Seq[Boolean]): Unit = {
    output.writeLong(v.size)
    var i = 0
    var written = 0
    val sz = v.size
    val it = v.iterator
    while (i < sz) {
      var l = 0.toByte
      var mask = 1.toByte
      val max = Math.min(i+8, sz)
      while (i < max) {
        if (it.next) l = (l | mask).toByte
        mask = (mask << 1).toByte
        i += 1
      }
      output.writeByte(l)
      written += 1
    }
    // write trailing bytes
    val byteSize = DenseIoBits.bitsToByteSize(sz)
    (written until byteSize.toInt).foreach { i => output.writeByte(0) }
  }
  override def writeMerged(output: DataOutputStream, a:DenseIoBits[Id], b:DenseIoBits[Id]) =
    DenseIoBits.writeMerged(output, a, b)
  override def writeAnyMerged(output: DataOutputStream, a:Any, b:Any) =
    DenseIoBits.writeAnyMerged(output, a, b)
  override def defaultSeq(lsize: Long): Any = DenseIoBits.defaultSeq(lsize)

  override def open(buf: IoData[Id]): DenseIoBits[Id] = {
    new DenseIoBits[Id](IoRef(this, buf.ref), buf.openRandomAccess)
  }
  override def valueTypeTag =
    _root_.scala.reflect.runtime.universe.typeTag[Boolean]
}

case class DenseBits(bits:util.BitSet, size:Long)

class DenseIoBitsType2[Id](implicit val t:TypeTag[DenseBits])
  extends IoTypeOf[Id, DenseIoBits[Id], DenseBits]()(t)
    with SeqIoType[Id, DenseIoBits[Id], Boolean] {

  override def write(output: DataOutputStream, v:DenseBits): Unit = {
    output.writeLong(v.size)
    val bs = v.bits.toByteArray
    bs.foreach { b =>
      output.writeByte(b)
    }
    val byteSize = DenseIoBits.bitsToByteSize(v.size)
    for (i <- bs.size until byteSize.toInt) {
      output.writeByte(0)
    }
  }
  override def writeMerged(output: DataOutputStream, a:DenseIoBits[Id], b:DenseIoBits[Id]) =
    DenseIoBits.writeMerged(output, a, b)
  override def writeAnyMerged(output: DataOutputStream, a:Any, b:Any) =
    DenseIoBits.writeAnyMerged(output, a, b)
  override def defaultSeq(lsize: Long): Any = DenseIoBits.defaultSeq(lsize)

  override def open(buf: IoData[Id]): DenseIoBits[Id] = {
    new DenseIoBits[Id](IoRef(this, buf.ref), buf.openRandomAccess)
  }
  override def valueTypeTag =
    _root_.scala.reflect.runtime.universe.typeTag[Boolean]
}

/**
  * Created by arau on 24.11.2016.
  */
class DenseIoBits[IoId](val ref:IoRef[IoId, DenseIoBits[IoId]], val origBuf:RandomAccess)
  extends IoBits[IoId] with java.io.Closeable {

  val bitSize = origBuf.getBeLong(0)

  val buf = origBuf.openSlice(8)

  def close = { origBuf.close; buf.close } //close both handles

  val longCount = DenseIoBits.bitsToLongCount(bitSize)

  def byteSize = buf.size

  def lsize = bitSize
  def offset = 8

  def long(l:Long) : Long = buf.getNativeLong(l*8)
//  def beLong(l:Long) : Long = buf.getBeLong(l*8)
  def leLong(l:Long) : Long = buf.getLeLong(l*8)

  def putLong(index:Long, l:Long) = buf.putNativeLong(index*8, l)

  def longs = for (pos <- (0L until longCount)) yield long(pos)
//  def beLongs = for (pos <- (0L until longCount)) yield beLong(pos)
  def leLongs = for (pos <- (0L until longCount)) yield leLong(pos)

  def f = {
    var rv = 0L
    val us = buf.unsafe
    var i = buf.address
    val end = i + 8*longCount
    while (i < end) {
      rv += java.lang.Long.bitCount(
        us.getLong(i)
      )
      i += 8
    }
    rv
  }

  def :=(bits:DenseIoBits[IoId]) = {
    if (bitSize != bits.bitSize) {
      throw new IllegalArgumentException("given bitset is of different length")
    }
    (0L until longCount).foreach { i =>
      putLong(i, bits.long(i))
    }
  }

  def &=(bits:DenseIoBits[IoId]) = {
    if (bitSize != bits.bitSize) {
      throw new IllegalArgumentException("given bitset is of different length")
    }
    (0L until longCount).foreach { i =>
      putLong(i, long(i) & bits.long(i))
    }
  }
  def fAnd(bits:IoBits[_]) = {
    bits match {
      case dense : DenseIoBits[_] => fAnd(dense)
      case sparse : SparseIoBits[_] => IoBits.fAnd(this, sparse)
      case b : EmptyIoBits[_] => 0
    }
  }

  def fAnd(bits:DenseIoBits[_]) = {
    if (bitSize != bits.bitSize) {
      throw new IllegalArgumentException("given bitset is of different length")
    }
    var rv = 0L
    buf.checkRange(0, 8*longCount)
    bits.buf.checkRange(0, 8*longCount)
    var i = buf.address
    var j = bits.buf.address
    val end = i + 8*longCount
    val us = buf.unsafe
    while (i < end) {
      rv += java.lang.Long.bitCount(
        us.getLong(i) & us.getLong(j)
      )
      i += 8
      j += 8
    }
    rv
  }

  def apply(i:Long) : Boolean = {
    if (i >= bitSize) throw new IllegalArgumentException(f"index $i out of [0, $bitSize]")
    val addr = i / 8L
    val offset = i % 8
    ((buf.getByte(addr) >> offset) & 1) == 1
  }

  def unsafeApply(i:Long) : Boolean = {
    val addr = i / 8L
    val offset = i % 8
    ((buf.unsafe.getByte(buf.address + addr) >> offset) & 1) == 1
  }

/*  def nextTrueBit(i:Long) : Boolean = {
    var l = i / 64
    var offset = i % 64
    var v = apply(l)
    while (v != 0) {
      l += 1
      v = apply(i)
      offset = 0
    }

  }*/

  val trues = new Iterable[Long]{
    override def iterator: Iterator[Long] = {
      new Iterator[Long] {
        var l = 0
        var v = buf.getByte(l)
        var n = 0L

        prepareNext()

        def prepareNext() {
          while (v == 0 && l + 1 < (lsize+7)/ 8) {
            l += 1
            v = buf.getByte(l)
          }
          val zeros = java.lang.Long.numberOfTrailingZeros(v)
          v = ((v & (~(1 << zeros))) & 0xff).toByte
          n = l*8 + zeros
          if (n >= lsize) {
            n = -1
          }
        }
        override def hasNext: Boolean = {
          n != -1
        }

        override def next(): Long = {
          val rv = n
          prepareNext
          rv
        }
      }
/*      new Iterator[Long] {
        var l = 0L
        var v = buf.getLeLong(l)
        var n = 0L

        prepareNext()

        def prepareNext() {
          while (v == 0L && l + 1 < longCount) {
            l += 1
            v = buf.getLeLong(l)
          }
          val zeros = java.lang.Long.numberOfTrailingZeros(v)
          v = (v & (~(1L << zeros)))
          n = l*64 + zeros
          if (n >= lsize) {
            n = -1L
          }
        }
        override def hasNext: Boolean = {
          n != -1L
        }

        override def next(): Long = {
          val rv = n
          prepareNext
          rv
        }
      }*/
    }
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

