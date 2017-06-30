package com.futurice.iodf.ioseq

import java.io.{Closeable, DataOutputStream}
import java.util

import com.futurice.iodf.Utils._
import com.futurice.iodf.store._
import com.futurice.iodf._
import com.futurice.iodf.io.{IoRef, SuperIoType}
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

  def apply(trues:Iterable[Long], size:Long)(implicit io:IoContext, scope:IoScope) = {
    scope.bind(io.bits.create(LBits(trues.toSeq, size))(io))
  }
  def apply(bits:Seq[Boolean])(implicit io:IoContext, scope:IoScope) = {
    scope.bind(io.bits.create(LBits(bits))(io))
  }
  def apply(bits:LBits)(implicit io:IoContext, scope:IoScope) = {
    scope.bind(io.bits.create(bits)(io))
  }
}

object IoBitsType {
  val SparseId = 0
  val DenseId = 1

  def booleanSeqIoType(bitsType:IoBitsType)(implicit t:TypeTag[Seq[Boolean]], valueTag:TypeTag[Boolean]) =
    new SuperIoType[Seq[Boolean], WrappedIoBits](
      bitsType,
      bools => LBits(bools))(t)
      with IoSeqType[Boolean, LBits, WrappedIoBits] {
      override def valueTypeTag: universe.TypeTag[Boolean] = valueTag
      def viewMerged(seqs: Seq[com.futurice.iodf.util.LBits]) =
        bitsType.viewMerged(seqs)
      def writeSeq(out: java.io.DataOutputStream,v: com.futurice.iodf.util.LBits) = {
        bitsType.writeSeq(out, v)
      }
    }


}

class WrappedIoBits(val someRef:Option[IoRef[IoBits]],
                          val bits:LBits) extends IoBits {
  def openRef = someRef.get

  def unwrap = bits
  override def close = {
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
      case w : WrappedIoBits[_] => w.bits
      case b => b
    }
  }
  override def createAnd[IoId2](b:LBits)(implicit io:IoContext[IoId2]) = {
    this.bits.createAnd(unwrap(b))
  }
  override def createAndNot[IoId2](b:LBits)(implicit io:IoContext[IoId2]) = {
    this.bits.createAndNot(unwrap(b))
  }
  override def createNot[IoId2](implicit io:IoContext[IoId2]) = {
    this.bits.createNot
  }
  override def createMerged[IoId2](b:LBits)(implicit io:IoContext[IoId2]) = {
    this.bits.createMerged(unwrap(b))
  }
  override def leLongs = bits.leLongs
  override def trues = bits.trues

}


class IoBitsType(val sparse:SparseIoBitsType,
                 val dense:DenseIoBitsType,
                 sparsityThreshold:Long = 4*1024)(implicit val t:TypeTag[LBits])
  extends IoSeqType[Boolean, LBits, WrappedIoBits] {

  import IoBitsType._

  override def valueTypeTag =
    _root_.scala.reflect.runtime.universe.typeTag[Boolean]

  def wrap(data:Option[FileDataRef], ioBits:LBits) = {
    new WrappedIoBits(
      data.map( ref => IoRef[IoId, IoBits](this, ref)),
      ioBits)
  }

  def open(buf:IoData) = {
    wrap(Some(buf.ref),
      using (buf.openRandomAccess) { ra =>
        ra.getByte(0) match {
          case SparseId =>
            using (buf.openView(1)) {
              sparse.open
            }
          case DenseId =>
            using (buf.openView(1)) {
              dense.open
           }
        }
      })
  }

  def write(out:DataOutputStream, data:LBits) = {
    if (LBits.isSparse(data.f, data.n)) {
      out.writeByte(SparseId)
      sparse.writeAny(out, data)
    } else {
      out.writeByte(DenseId)
      dense.writeAny(out, data)
    }
  }
  def writeSeq(out:DataOutputStream, data:LBits) = {
    write(out, data)
  }

  override def defaultSeq(lsize:Long) = {
    Some(LBits.empty(lsize))
  }

  def unwrap(b:LBits) = {
    b match {
      case w : WrappedIoBits[_] => w.unwrap
      case _ => b
    }
  }

  override def viewMerged(seqs:Seq[LBits]) = {
    MultiBits(seqs)
  }

  def create(bits:LBits)(implicit io: IoContext) : IoBits = {
    val ref = io.dir.freeRef
    using( ref.openOutput ) { output =>
      write(new DataOutputStream(output), bits)
    }
    using(ref.open) { open(_) }
  }

  def writeAnd(output: DataOutputStream, b1:LBits, b2:LBits) : IoSeqType[IoId, Boolean, LBits, _ <: IoBits] = {
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
  def createAnd(file: FileRef, b1:LBits, b2:LBits) : IoBits = {
    val typ = using( file.openOutput) { output =>
      writeAnd(new DataOutputStream(output), b1, b2)
    }
    using(file.open) { typ.open(_) }
  }
  def createAnd(dir: Dir, b1:LBits, b2:LBits) : IoBits = {
    createAnd(dir.freeRef, b1, b2)
  }

  def writeAndNot(output: DataOutputStream, b1:LBits, b2:LBits) : IoSeqType[IoId, Boolean, LBits, _ <: IoBits] = {
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
  def createAndNot(file: FileRef, b1:LBits, b2:LBits) : IoBits = {
    val typ = using( file.openOutput) { output =>
      writeAndNot(new DataOutputStream(output), b1, b2)
    }
    using(file.open) { typ.open(_) }
  }
  def createAndNot(dir: Dir, b1:LBits, b2:LBits) : IoBits = {
    createAndNot(FileRef(dir, dir.freeId), b1, b2)
  }

  def writeNot(output: DataOutputStream, b:LBits) : IoSeqType[IoId, Boolean, LBits, _ <: IoBits] = {
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
  def createNot(ref: DataCreatorRef, b:LBits) : IoBits = {
    val (typ, res) = using( ref.create ) { out =>
      val t = writeNot(new DataOutputStream(out), b)
      (t, out.adoptResult)
    }
    using(res) { typ.open(_) }
  }
  def createNot(dir: DataLand, b:LBits) : IoBits = {
    createNot(FileRef(dir, dir.openRef), b)[IoId]
  }

  override def writeMerged(out:DataOutputStream, ss:Seq[LBits]) = {
    val f = ss.map(_.f).sum
    val n = ss.map(_.n).sum
    if (LBits.isSparse(f, n)) {
      out.writeByte(SparseId)
      sparse.writeMerged(out, ss.map(unwrap))
    } else {
      out.writeByte(DenseId)
      dense.writeMerged(out, ss.map(unwrap))
    }
  }

  def createMerged(refe: DataCreatorRef, bs:Seq[LBits]) : IoBits = {
    using( res ) { output =>
      writeMerged(new DataOutputStream(output), bs)
    }
    using(file.open) { open(_) }
  }
  def createMerged(dir: Dir, bs:Seq[LBits]) : IoBits = {
    createMerged(dir.freeRef, bs)
  }

}

