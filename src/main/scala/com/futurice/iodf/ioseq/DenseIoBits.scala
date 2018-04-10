package com.futurice.iodf.ioseq

import java.util

import com.futurice.iodf.store._
import com.futurice.iodf.util._
import com.futurice.iodf._
import com.futurice.iodf.io.{DataAccess, DataOutput, DataRef, IoRef}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._


object DenseIoBits {
  def bitsToLongCount(bitSize:Long) = ((bitSize+63L) / 64L)
  def bitsToByteSize(bitSize:Long) = bitsToLongCount(bitSize) * 8

}

// TODO: there are three different mappings from different data structures
//       to dense bits. These should be merged !!!


class DenseIoBitsType
  extends SeqIoType[Boolean, LBits, DenseIoBits] {

  def interfaceType = typeOf[LBits]
  def ioInstanceType = typeOf[DenseIoBits]
  def valueTypeTag = implicitly[TypeTag[Boolean]]

  def defaultSeq[Id](lsize:Long) = {
    LBits.empty(lsize)
  }
  def writeMerged2(out: DataOutput,
                   sizeLeLongs:Seq[(Long, Iterator[Long])]) = {
    out.writeLong(sizeLeLongs.map(_._1).sum)

    var overbits = 0
    var overflow = 0L

    def writeLong(v:Long) = {
      out.writeLeLong((v << overbits) | overflow)
      overflow = (v >>> (64 - overbits)).toLong
    }

    sizeLeLongs.foreach { case (lsize, les) =>
      val readLongs = (lsize / 64)
      if (overbits > 0) {
        (0L until readLongs).foreach { l =>
          writeLong(les.next)
        }
      } else {
        (0L until readLongs).foreach { l =>
          out.writeLeLong(les.next)
        }
      }
      var extra = lsize % 64
      val left = overbits + extra
      if (left >= 64) { // there is one word full of content
        out.writeLeLong(les.next)
      } else if (les.hasNext) { // pad the extra bits to overflow
        overflow = ((les.next() << overbits) | overflow)
      }
      overbits = (left % 64).toInt
    }
    if (overbits > 0) {
      out.writeLeLong(overflow)
    }
  }
  override def writeMerged(out: DataOutput, bs:Seq[Ref[LBits]]): Unit = {
    writeMerged2(out, bs.map(e => (e.get.n, e.get.leLongs.iterator)))
  }

  def writeLSeq(output: DataOutput, v:LSeq[Boolean]): Unit = {
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
  def write(output: DataOutput, bitN:Long, leLongs:Iterator[Long]): Unit = {
    output.writeLong(bitN)
    for (l <- leLongs) output.writeLeLong( l )
  }
  def writeSeq(output:DataOutput, v:LBits) : Unit = {
    write(output, v.lsize, v.leLongs.iterator)
  }
  override def write(output: DataOutput, v:LBits): Unit = {
    writeSeq(output, v)
  }

  def write(output: DataOutput, v:DenseBitStream): Unit = {
    write(output, v.n, v.leLongs)
  }

  override def apply(buf: DataAccess): DenseIoBits = {
    new DenseIoBits(IoRef.open(this, buf.dataRef), buf)
  }

  override def openViewMerged(seqs: Seq[Ref[LBits]]): LBits = MultiBits.open(seqs)
}

/*
class BooleanIoSeqType[Id](implicit val t:TypeTag[Seq[Boolean]])
  extends IoTypeOf[Id, DenseIoBits[Id], Seq[Boolean]]()(t)
    with IoSeqType[Id, LBits, DenseIoBits[Id], Boolean] {

  override def write(output: DataOutput, v:Seq[Boolean]): Unit = {
    DenseIoBits.write(output, v)
  }
  override def writeMerged(output: DataOutput, a:DenseIoBits[Id], b:DenseIoBits[Id]) =
    DenseIoBits.writeMerged(output, a, b)
  override def defaultSeq(lsize: Long) = DenseIoBits.defaultSeq(lsize)
 override def open(buf: IoData[Id]): DenseIoBits[Id] = {
    new DenseIoBits[Id](IoRef(this, buf.ref), buf.openRandomAccess)
  }
  override def valueTypeTag =
    _root_.scala.reflect.runtime.universe.typeTag[Boolean]
  override def viewMerged(seqs: Array[LBits]): LBits = new MultiBits(seqs)
}*/


/**
  * Created by arau on 24.11.2016.
  */
class DenseIoBits(_openRef:IoRef[DenseIoBits], _origBuf:DataAccess)
  extends IoBits with java.io.Closeable {

  val origBuf = _origBuf.openCopy

  Tracing.opened(this)

  val bitSize = origBuf.getBeLong(0)
  override val longCount = DenseIoBits.bitsToLongCount(bitSize)

  val buf = origBuf.view(8, 8 + longCount*8)

  override def close = {
    Tracing.closed(this)
    _openRef.close; origBuf.close; buf.close
  } //close both handles

  def ref = _openRef.openCopy

  def byteSize = buf.size

  def lsize = bitSize
  def offset = 8

  def long(l:Long) : Long = buf.getNativeLong(l*8)
  def leLong(l:Long) : Long = buf.getLeLong(l*8)

  override def isDense = true

  def longs =
    new Iterable[Long] {
      def iterator = new Iterator[Long] {
        var at = 0L
        def hasNext = at < longCount
        def next = {
          val rv = long(at)
          at += 1
          rv
        }
      }
    }

  override def leLongs =
    new Iterable[Long] {
      def iterator = new Iterator[Long] {
        var at = 0L
        def hasNext = at < longCount
        def next = {
          val rv = leLong(at)
          at += 1
          rv
        }
      }
    }

  override lazy val f = {
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

  override def fAnd(bits:LBits) : Long = {
    bits match {
      case wrap : WrappedIoBits => fAnd(wrap.unwrap)
      case dense : DenseIoBits => fAnd(dense)
      case sparse : SparseIoBits => IoBits.fAndDenseSparse(this, sparse)
      case _ =>
        LBits.fAnd(this, bits)
    }
  }

  def fAnd(bits:DenseIoBits) = {
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

  override def apply(i:Long) : Boolean = {
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

  private def truesFromBit(bitPos:Long) : Scanner[Long,Long] = new Scanner[Long,Long] {
    if (bitPos > lsize || bitPos < 0) throw new IllegalArgumentException(f"position $bitPos is outside bounds [0, $bitSize]")
    // TODO: this moves forward byte at a time. Shouldn't this operate on (le)longs?
    var l : Long = bitPos / 8
    var v = {
      val mask = 0xff << (bitPos % 8)
      buf.getByte(l) & mask
    }
    var n : Long = bitPos

    prepareNext()

    def copy = truesFromBit(n)

    def prepareNext() {
      // FIXME: this would be faster with longs instead of bytes
      while (v == 0 && l + 1 < (lsize+7)/ 8) {
        l += 1
        v = buf.getByte(l)
      }
      val zeros = java.lang.Long.numberOfTrailingZeros(v)
      v = ((v & (0xff << (zeros+1))) & 0xff).toByte
      n = l*8 + zeros
      if (n >= lsize) {
        n = lsize
      }
    }
    override def hasNext: Boolean = {
      n < lsize
    }
    override def seek(target:Long) = {
      if (target < 0 || target > bitSize) {
        throw new IllegalArgumentException(f"seek $target outside bounds [0, $bitSize]")
      }
      n = target
      if (target < lsize) {
        l = target / 8
        val clear = target % 8
        v = (buf.getByte(l) & (0xff << clear)).toByte
        prepareNext
      }
      n == target
    }
    override def headOption = n match {
      case v if v == lsize => None
      case v  => Some(v)
    }
    override def head = n

    override def next(): Long = {
      val rv = n
      prepareNext
      rv
    }
  }

  val trues = new ScannableLSeq[Long, Long]{
    override def iterator = truesFromBit(0)

    override def apply(l: Long) : Long = {
      var left = l
      var i = 0L
      while (true) {
        var word = buf.getNativeLong(i)
        val f = java.lang.Long.bitCount(word)
        if (left < f) {
          while (true) {
            val truePos = java.lang.Long.numberOfTrailingZeros(word)
            if (left == 0) return (i*64) + truePos
            word = word & (0xffffffffffffffffL << (truePos+1)) // clear the found bit
          }
        } else {
          left -= f
          i += 1
        }
      }
      throw new RuntimeException("unreachable")
    }

    override def lsize = f
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

