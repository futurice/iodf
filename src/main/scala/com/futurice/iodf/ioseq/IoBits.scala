package com.futurice.iodf.ioseq

import java.io.Closeable

import com.futurice.iodf.Utils._
import com.futurice.iodf.store._
import com.futurice.iodf._
import com.futurice.iodf.io._
import com.futurice.iodf.util._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._


/**
  * Created by arau on 15.12.2016.
  */
trait IoBits extends IoSeq[Boolean] with LBits {

  def trues : Scannable[Long, Long]

  override def n = lsize
  def f : Long = trues.size
  def apply(index:Long)= {
    trues.iterator.seek(index)
  }
}

object IoBits {

  def fAndDenseSparse(dense:DenseIoBits, sparse: SparseIoBits) = {
    if (dense.lsize != sparse.lsize) throw new RuntimeException("fAnd operation on bitsets of different sizes")
    var rv = 0L
    val ts = sparse.indexes
    val sz = ts.lsize*8L
    ts.buf.checkRange(ts.offset, sz)
    var i = ts.buf.address + ts.offset
    val end = i + sz
    while (i < end) {
      val t = ts.buf.unsafeGetBeLongByAddress(i)
      if (dense.apply(t)) rv += 1
      i += 8
    }
    rv
  }

  def apply(trues:Iterable[Long], size:Long)(implicit io:IoContext, bind:IoScope) = {
    bind(create(LBits(trues.toSeq, size)))
  }
  def apply(bits:Seq[Boolean])(implicit io:IoContext, bind:IoScope) = {
    bind(create(LBits(bits)))
  }
  def apply(bits:LBits)(implicit io:IoContext, bind:IoScope) = {
    bind(create(bits))
  }
  def create(bits:LBits)(implicit io:IoContext) = {
    io.bits.create(bits)(io)
  }

}

object BitsIoType {
  val SparseId = 0
  val DenseId = 1

  def booleanSeqIoType(bitsType:BitsIoType)(implicit t:TypeTag[LSeq[Boolean]], valueTag:TypeTag[Boolean]) =
    new SeqIoType[Boolean, LSeq[Boolean], WrappedIoBits] {
      val tp =
        new SuperIoType[LSeq[Boolean], LBits, WrappedIoBits](
          bitsType,
          _ match {
            case bits : LBits => bits
            case bools => LBits(bools)
          })
      override def valueTypeTag: universe.TypeTag[Boolean] = valueTag
      def viewMerged(seqs: Seq[Ref[LSeq[Boolean]]]) =
        bitsType.viewMerged(seqs.map { _.as { _ match {
          case bits : LBits => bits
          case bools : LSeq[Boolean] => LBits(bools)
        }}})
      override def interfaceType: universe.Type = tp.interfaceType
      override def ioInstanceType: universe.Type = tp.ioInstanceType

      override def open(ref: DataAccess): WrappedIoBits = tp.open(ref)

      override def write(out: DataOutput, iface: LSeq[Boolean]): Unit = tp.write(out, iface)
    }

}

class WrappedIoBits(val someRef:Option[IoRef[IoBits]],
                    val bits:LBits) extends IoBits {
  def openRef = someRef.get.openCopy

  def unwrap = bits
  override def close = {
    someRef.foreach { _.close }
    bits match {
      case c : Closeable => c.close
    }
  }
  override def apply(l:Long) = bits.apply(l)
  def lsize = bits.lsize
  override def f = bits.f
  override def fAnd(bits:LBits) = this.bits.fAnd(unwrap(bits))
  def unwrap(b:LBits) = {
    b match {
      case w : WrappedIoBits => w.bits
      case b => b
    }
  }
  override def createAnd(b:LBits)(implicit io:IoContext) = {
    this.bits.createAnd(unwrap(b))
  }
  override def createAndNot(b:LBits)(implicit io:IoContext) = {
    this.bits.createAndNot(unwrap(b))
  }
  override def createNot(implicit io:IoContext) = {
    this.bits.createNot
  }
  override def createMerged(b:LBits)(implicit io:IoContext) = {
    this.bits.createMerged(unwrap(b))
  }
  override def leLongs = bits.leLongs
  override def trues = bits.trues

}


class BitsIoType(val sparse:SparseIoBitsType,
                 val dense:DenseIoBitsType,
                 sparsityThreshold:Long = 4*1024)(implicit val t:TypeTag[LBits])
  extends SeqIoType[Boolean, LBits, WrappedIoBits] {

  import BitsIoType._

  override def valueTypeTag =
    _root_.scala.reflect.runtime.universe.typeTag[Boolean]

  override def interfaceType = typeOf[LBits]
  override def ioInstanceType = typeOf[WrappedIoBits]

  def wrap(data:Option[DataAccess], ioBits:LBits) = {
    new WrappedIoBits(
      data.map( ref => IoRef.open(this, ref.dataRef)),
      ioBits)
  }

  def open(data:DataAccess) = {
    wrap(
      Some(data),
      data.getByte(0) match {
          case SparseId =>
            using (data.openView(1, data.size)) {
              sparse.open
            }
          case DenseId =>
            using (data.openView(1, data.size)) {
              dense.open
           }
        })
  }

  def write(out:DataOutput, data:LBits) = {
    if (LBits.isSparse(data.f, data.n)) {
      out.writeByte(SparseId)
      sparse.write(out, data)
    } else {
      out.writeByte(DenseId)
      dense.write(out, data)
    }
  }
  def writeSeq(out:DataOutput, data:LBits) = {
    write(out, data)
  }

  override def defaultInstance(lsize:Long) = {
    Some(LBits.empty(lsize))
  }

  def unwrap(b:LBits) = {
    b match {
      case w : WrappedIoBits => w.unwrap
      case _ => b
    }
  }

  override def viewMerged(seqs:Seq[Ref[LBits]]) = {
    new MultiBits(seqs.toArray)
  }

  def create(bits:LBits)(implicit io: IoContext) : IoBits = {
    using (using( io.allocator.create ) { out =>
        write(out, bits)
        out.openDataRef
      }) { ref =>
      open(ref)
    }
  }

  def writeAnd(output: DataOutput, b1:LBits, b2:LBits) : SeqIoType[Boolean, LBits, _ <: IoBits] = {
    if (b1.n != b2.n) throw new IllegalArgumentException()
    (b1.isDense, b2.isDense) match {
      case (true, true) =>
        dense.write(output, b1.lsize,
          new Iterator[Long] {
            val i = b1.leLongs.iterator
            val j = b2.leLongs.iterator

            def hasNext = i.hasNext
            def next = i.next & j.next
          })
        dense
      case (true, false) =>
        sparse.write(output, LBits(b2.trues.iterator.filter(b1(_)).toArray, b1.n))
        sparse
      case (false, true) =>
        sparse.write(output, LBits(b1.trues.iterator.filter(b2(_)).toArray, b1.n))
        sparse
      case (false, false) =>
        val trues = ArrayBuffer[Long]()
        var i = b1.trues.iterator
        var j = b2.trues.iterator
        while (i.hasNext && j.hasNext) {
          val t1 = i.head
          val t2 = j.head
          if (t1 < t2) i.seek(t2)
          else if (t1 > t2) j.seek(t1)
          else {
            trues += t1
            i.next
            j.next
          }
        }
        sparse.write(output, LBits(trues, b1.n))
        sparse
    }
  }
  def createAnd(allocator: AllocateOnce, b1:LBits, b2:LBits) : IoBits = {
    val (typ, openRef) = using( allocator.create ) { output =>
      val t = writeAnd(output, b1, b2)
      (t, output.openDataRef)
    }
    using(openRef) { typ.open(_) }
  }

  def writeAndNot(output: DataOutput, b1:LBits, b2:LBits) : SeqIoType[Boolean, LBits, _ <: IoBits] = {
    if (b1.n != b2.n) throw new IllegalArgumentException()
    (b1.isDense, b2.isDense) match {
      case (true, true) =>
        dense.write(output, b1.lsize,
          new Iterator[Long] {
            val i = b1.leLongs.iterator
            val j = b2.leLongs.iterator

            def hasNext = i.hasNext

            def next = i.next & ~j.next
          })

        dense
      case (true, false) =>
        dense.write(output, b1.lsize,
          new Iterator[Long] {
            var les = b1.leLongs.iterator
            var trues = b2.trues.iterator
            var begin = 0L

            def next = {
              var rv = les.next
              val end = begin + 64L
              while (trues.hasNext && trues.head < end) {
                rv &= ~(1L<<(trues.next-begin))
              }
              begin += 64L
              rv
            }
            def hasNext = les.hasNext
          })
        dense
      case (false, true) =>
        sparse.write(output, LBits(b1.trues.filter(!b2(_)).toSeq, b1.n))
        sparse
      case (false, false) =>
        val trues = ArrayBuffer[Long]()
        var i = b1.trues.iterator
        var j = b2.trues.iterator
        while (i.hasNext && j.hasNext) {
          val v1 = i.head
          val v2 = j.head
          if (v1 == v2) {
            i.next
            j.next
          } else if (v1 < v2) {
            trues += v1
            i.next
          } else {
            j.next
          }
        }
        while (i.hasNext) trues += i.next
        sparse.write(output, LBits(trues, b1.n))
        sparse
    }
  }
  def createAndNot(file: AllocateOnce, b1:LBits, b2:LBits) : IoBits = {
    val (typ, openRef) = using( file.create ) { output =>
      val t = writeAndNot(output, b1, b2)
      (t, output.openDataRef)
    }
    using(openRef) { ref => typ.open(ref) }
  }

  def writeNot(output: DataOutput, b:LBits) : SeqIoType[Boolean, LBits, _ <: IoBits] = {
    dense.write(output, b.size,
      b.leLongs.iterator.zipWithIndex.map { case (l, i) =>
        if ((i+1)*64 > b.lsize) {
          val bits = b.lsize - i*64
          val mask =  ((~0L) >>> (64-bits))
          (~l) & mask
        } else {
          ~l
        }
      }
    )
    dense
  }
  def createNot(ref: AllocateOnce, b:LBits) : IoBits = {
    val (typ, openRef) = using( ref.create ) { out =>
      val t = writeNot(out, b)
      (t, out.openDataRef)
    }
    using(openRef) { typ.open(_) }
  }

  override def writeMerged(out:DataOutput, ss:Seq[Ref[LBits]]) = {
    val f = ss.map(_.get.f).sum
    val n = ss.map(_.get.n).sum
    if (LBits.isSparse(f, n)) {
      out.writeByte(SparseId)
      sparse.writeMerged(out, ss.map(_.as(b => unwrap(b))))
    } else {
      out.writeByte(DenseId)
      dense.writeMerged(out, ss.map(_.as(b => unwrap(b))))
    }
  }

  def createMerged(ref: AllocateOnce, bs:Seq[Ref[LBits]]) : IoBits = {
    val openRes =
      using( ref.create ) { out =>
        writeMerged(out, bs)
        out.openDataRef
      }
    using(openRes) { open(_) }
  }

}

