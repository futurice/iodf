package com.futurice.iodf.ioseq

import java.io.DataOutputStream
import java.util

import com.futurice.iodf.Utils._
import com.futurice.iodf.store._
import com.futurice.iodf._
import com.futurice.iodf.utils._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._


/**
  * Created by arau on 15.12.2016.
  */
trait IoBits[IoId] extends IoSeq[IoId, Boolean] {

  def n = lsize
  def f : Long
  def fAnd(bits:IoBits[_]) : Long
  def leLongs : Iterable[Long]

  def longCount = DenseIoBits.bitsToLongCount(n)

  def trues : Iterable[Long]

  def createAnd[IoId1, IoId2](b:IoBits[IoId1])(implicit io:IoContext[IoId2]) = {
    io.bits.createAnd(io.dir, this, b)
  }
  def createAndNot[IoId1, IoId2](b:IoBits[IoId1])(implicit io:IoContext[IoId2]) = {
    io.bits.createAndNot(io.dir, this, b)
  }
  def createNot[IoId1, IoId2](implicit io:IoContext[IoId2]) = {
    io.bits.createNot(io.dir, this)
  }
  def createMerged[IoId1, IoId2](b:IoBits[IoId1])(implicit io:IoContext[IoId2]) = {
    io.bits.createMerged(io.dir, this, b)
  }
  def &[IoId1, IoId2](b:IoBits[IoId1])(implicit io:IoContext[IoId2], scope:IoScope) = {
    scope.bind(createAnd(b))
  }
  def &~[IoId1, IoId2](b:IoBits[IoId1])(implicit io:IoContext[IoId2], scope:IoScope) = {
    scope.bind(createAndNot(b))
  }
  def ~[IoId1](implicit io:IoContext[IoId1], scope:IoScope) = {
    scope.bind(createNot)
  }
  def merge [IoId1, IoId2](b:IoBits[IoId1])(implicit io:IoContext[IoId2], scope:IoScope) : IoBits[IoId2] = {
    scope.bind(createMerged(b))
  }
}

class EmptyIoBits[IoId](val lsize : Long) extends IoBits[IoId] {
  override def f: Long = 0
  override def fAnd(bits: IoBits[_]): Long = 0
  override def trues: Iterable[Long] = Seq[Long]()
  override def leLongs = new Iterable[Long] {
    def iterator = new Iterator[Long] {
      var l = 0
      val max = DenseIoBits.bitsToLongCount(lsize)
      def hasNext = l < max
      def next = {
        l += 1
        0L
      }
    }
  }
  override def apply(l: Long): Boolean = false
  override def ref: IoRef[IoId, _ <: IoObject[IoId]] =
    throw new RuntimeException("not referable")
  override def close(): Unit = {}
}

object IoBits {

  def denseSparseSplit = 128L

  def isDense(f:Long, n:Long) = {
    f * denseSparseSplit > n
  }
  def isDense(bits:IoBits[_]) : Boolean = isDense(bits.f, bits.n)

  def isSparse(f:Long, n:Long) : Boolean = !isDense(f, n)
  def isSparse(bits:IoBits[_]) : Boolean = isSparse(bits.f, bits.n)

  def fAnd(a:IoBits[_], b:IoBits[_]): Long = {
    (isSparse(a), isSparse(b)) match {
      case (false, false) => fAndDenseDense(a, b)
      case (false, true) => fAndSparseDense(b, a)
      case (true, false) => fAndSparseDense(a, b)
      case (true, true) => fAndSparseSparse(a, b)
    }
  }
  def fAndDenseDense(a:IoBits[_], b : IoBits[_]): Long = {
    val at = a.leLongs.iterator
    val bt = a.leLongs.iterator
    var rv = 0
    while (at.hasNext) {
      rv += java.lang.Long.bitCount(at.next() & bt.next())
    }
    rv
  }
  def fAndSparseDense(sparse : IoBits[_], dense : IoBits[_]): Long = {
    var rv = 0L
    // assume dense to be random accessible
    for (t <- sparse.trues) {
      if (dense(t)) rv += 1L
    }
    rv
  }
  def fAndSparseSparse(a:IoBits[_], b : IoBits[_]): Long = {
    var rv = 0L
    val i1 = PeekIterator(a.trues.iterator)
    val i2 = PeekIterator(b.trues.iterator)
    while (i1.hasNext && i2.hasNext) {
      val t1 = i1.head
      val t2 = i2.head
      if (t1 < t2) i1.next
      else if (t1 > t2) i2.next
      else {
        rv += 1
        i1.next
        i2.next
      }
    }
    rv
  }


  def fAndDenseSparse(dense:DenseIoBits[_], sparse: SparseIoBits[_]) = {
    if (dense.lsize != sparse.lsize) throw new RuntimeException("fAnd operation on bitsets of different sizes")
    var rv = 0L
    val ts = sparse.trues
    val sz = ts.lsize*8
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

  def apply[IoId](trues:Iterable[Long], size:Long)(implicit io:IoContext[IoId], scope:IoScope) = {
    scope.bind(io.bits.create(trues, size)(io))
  }

  def apply[IoId](bits:Seq[Boolean])(implicit io:IoContext[IoId], scope:IoScope) = {
    scope.bind(io.bits.create(bits)(io))
  }
  def apply[IoId](bits:Bits)(implicit io:IoContext[IoId], scope:IoScope) = {
    scope.bind(io.bits.create(bits)(io))
  }
}

object IoBitsType {
  val SparseId = 0
  val DenseId = 1
}

object MultiIoBits {
//  def apply[IoId](bits:Array[IoBits[_]]) = new MultiIoBits[IoId](bits)
  def apply[IoId](bits:Seq[IoBits[_]]) = new MultiIoBits[IoId](bits.toArray)
}

class MultiIoBits[IoId](val bits:Array[IoBits[_]]) extends MultiSeq[IoId, Boolean, IoBits[_]](bits) with IoBits[IoId] {
  override def f: Long = {
    bits.map(_.f).sum
  }

  def mapOperation[T, E](bs: IoBits[_],
                         map:(IoBits[_], IoBits[_])=>T,
                         reduce:Seq[T]=>E,
                         fallback:(IoBits[_], IoBits[_])=>E) = {
    bs match {
      case b : MultiIoBits[_] =>
        reduce((bits zip b.bits).map { case (a, b) => map(a, b) })
      case _ =>
        fallback(this, bs)
    }
  }


  override def fAnd(bs: IoBits[_]): Long = {
    mapOperation(bs, _ fAnd _, (vs : Seq[Long]) => vs.sum, IoBits.fAnd(_, _))
  }
  override def leLongs: Iterable[Long] = {
    new MultiIterable[Long](bits.map(_.leLongs))
  }
  override def trues: Iterable[Long] = {
    new MultiIterable[Long](bits.map(_.trues))
  }

  override def createAnd[IoId1, IoId2](b:IoBits[IoId1])(implicit io:IoContext[IoId2]) = {
    mapOperation(
      b, _ createAnd _, MultiIoBits.apply _,
      (a, b) => io.bits.createAnd(io.dir.ref(io.dir.freeId), a, b))
  }
  override def createAndNot[IoId1, IoId2](b:IoBits[IoId1])(implicit io:IoContext[IoId2]) = {
    mapOperation(b, _ createAndNot _, MultiIoBits.apply _,
      (a, b) => io.bits.createAndNot(io.dir.ref(io.dir.freeId), a, b))
  }
  override def createNot[IoId1, IoId2](implicit io:IoContext[IoId2]) = {
    MultiIoBits[IoId2](bits.map(_.createNot))
  }
  override def createMerged[IoId1, IoId2](b:IoBits[IoId1])(implicit io:IoContext[IoId2]) = {
    io.bits.createMerged(io.dir, this, b)
  }

}

class WrappedIoBits[IoId](val someRef:Option[IoRef[IoId, IoBits[IoId]]],
                          val bits:IoBits[IoId]) extends IoBits[IoId] {
  def ref = someRef.get

  def unwrap = bits
  def close = bits.close
  def apply(l:Long) = bits.apply(l)
  def lsize = bits.size
  def f = bits.f
  def fAnd(bits:IoBits[_]) = this.bits.fAnd(bits)
  def leLongs = bits.leLongs
  def trues = bits.trues

}


class IoBitsType[IoId](val sparse:SparseIoBitsType[IoId],
                       val dense:DenseIoBitsType[IoId],
                       sparsityThreshold:Long = 4*1024)(implicit val t:TypeTag[Bits])
  extends IoTypeOf[IoId, WrappedIoBits[IoId], Bits]
   with SeqIoType[IoId, WrappedIoBits[IoId], Boolean] {

  import IoBitsType._

/*  override def asTypeOf[E](implicit t:Type) = {
    sparse.asTypeOf[E](t) match {
      case Some(v) => Some(this.asInstanceOf[IoTypeOf[IoId, IoBits[IoId], E]])
      case None => dense.asTypeOf[E](t) match {
        case Some(v) =>
        case None => Some(this.asInstanceOf[IoTypeOf[IoId, IoBits[IoId], E]])
      }
    }
  }*/

  override def valueTypeTag =
    _root_.scala.reflect.runtime.universe.typeTag[Boolean]

  def wrap(data:Option[DataRef[IoId]], ioBits:IoBits[IoId]) = {
    new WrappedIoBits(
      data.map( ref => IoRef[IoId, IoBits[IoId]](this, ref)),
      ioBits)
  }

  def open(buf:IoData[IoId]) = {
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

  def write(out:DataOutputStream, data:Bits) = {
    if (IoBits.isSparse(data.f, data.n)) {
      out.writeByte(SparseId)
      sparse.writeAny(out, data)
    } else {
      out.writeByte(DenseId)
      dense.writeAny(out, data)
    }
  }

  def writeDense(output: DataOutputStream, bools:Seq[Boolean]) = {
    dense.write(output, bools)
    dense
  }

  override def defaultSeq(lsize:Long) = {
    wrap(None, new EmptyIoBits[IoId](lsize))
  }

  def unwrap[Id](b:IoBits[Id]) = {
    b match {
      case w : WrappedIoBits[Id] => w.unwrap
      case _ => b
    }
  }

  override def viewMerged(seqs:Array[IoSeq[IoId, Boolean]]) = {
    new MultiIoBits[IoId](seqs.map(_.asInstanceOf[IoBits[IoId]]))
  }


  def writeMerged(out:DataOutputStream, seqA:WrappedIoBits[IoId], seqB:WrappedIoBits[IoId]) = {
    writeMerged2(out, seqA, seqB)
  }

  def writeMerged2(out:DataOutputStream, seqA:IoBits[_], seqB:IoBits[_]) = {
    if (IoBits.isSparse(seqA.f + seqB.f, seqA.n + seqB.n)) {
      out.writeByte(SparseId)
      sparse.writeAnyMerged(out, unwrap(seqA), unwrap(seqB))
    } else {
      out.writeByte(DenseId)
      dense.writeAnyMerged(out, unwrap(seqA), unwrap(seqB))
    }
  }

  def createDense(file:FileRef[IoId], bools:Seq[Boolean]): IoBits[IoId] = {
    val typ = using( file.openOutput ) { output =>
      writeDense(new DataOutputStream(output), bools)
    }
    using(file.open) { typ.open(_) }
  }

  def createDense(dir: Dir[IoId], bools:Seq[Boolean]) : IoBits[IoId] = {
    createDense(FileRef(dir, dir.freeId), bools)
  }

  def createDense(io: IoContext[IoId], bools:Seq[Boolean]) : IoBits[IoId] = {
    createDense(io.dir, bools.toSeq)
  }

  def writeSparse(output: DataOutputStream, trues:Iterable[Long], size:Long) = {
    sparse.write(output, SparseBits(trues, size))
    sparse
  }

  def createSparse(file:FileRef[IoId], trues:Iterable[Long], size:Long): IoBits[IoId] = {
    val typ = using( file.openOutput) { output =>
      writeSparse(new DataOutputStream(output), trues, size)
    }
    using(file.open) { typ.open(_) }
  }

  def createSparse(dir: Dir[IoId], trues:Iterable[Long], size:Long) : IoBits[IoId] = {
    createSparse(FileRef(dir, dir.freeId), trues, size)
  }


  def create(trues:Iterable[Long], size:Long)(implicit io: IoContext[IoId]) : IoBits[IoId] = {
    if (IoBits.isSparse(trues.size, size)) {
      createSparse(io.dir, trues, size)
    } else {
      val lookup = trues.toSet
      createDense(io.dir, (0L until size).map(lookup))
    }
  }

  def create(bits:Seq[Boolean])(implicit io: IoContext[IoId]) : IoBits[IoId] = {
    if (IoBits.isSparse(bits.count(e => e), bits.size)) {
      createSparse(io.dir, bits.zipWithIndex.filter(_._1).map(_._2.toLong), bits.size)
    } else {
      createDense(io.dir, bits)
    }
  }

  def create(bits:Bits)(implicit io: IoContext[IoId]) : IoBits[IoId] = {
    if (IoBits.isSparse(bits.f, bits.size)) {
      createSparse(io.dir, bits.trues, bits.size)
    } else {
      createDense(io.dir, bits.toSeq)
    }
  }

  def writeAnd[IoId1, IoId2](output: DataOutputStream, b1:IoBits[IoId1], b2:IoBits[IoId2]) : SeqIoType[IoId, _ <: IoBits[IoId], Boolean] = {
    if (b1.n != b2.n) throw new IllegalArgumentException()
    (IoBits.isDense(b1), IoBits.isDense(b2)) match {
      case (true, true) =>
        dense.write(output, b1.lsize,
          new Iterable[Long] {
            def iterator = new Iterator[Long] {
              val i = b1.leLongs.iterator
              val j = b2.leLongs.iterator

              def hasNext = i.hasNext

              def next = i.next & j.next
            }
          })
//          (b1.leLongs zip b2.leLongs).map { case (a, b) => a & b } )
        dense
      case (true, false) =>
        sparse.write(output, new SparseBits(b2.trues.filter(b1(_)), b1.n))
        sparse
      case (false, true) =>
        sparse.write(output, new SparseBits(b1.trues.filter(b2(_)), b1.n))
        sparse
      case (false, false) =>
        val trues = ArrayBuffer[Long]()
        var i = PeekIterator[Long](b1.trues.iterator)
        var j = PeekIterator[Long](b2.trues.iterator)
        while (i.hasNext && j.hasNext) {
          val t1 = i.head
          val t2 = j.head
          if (t1 < t2) i.next
          else if (t1 > t2) j.next
          else {
            trues += t1
            i.next
            j.next
          }
        }
        sparse.write(output, new SparseBits(trues, b1.n))
        sparse
    }

    /*    (unwrap(b1), unwrap(b2)) match {
      case (d1 : DenseIoBits[IoId1], d2 : DenseIoBits[IoId2]) =>
        dense.write(output, d1.size, (0L until d1.longCount).map { i =>
          d1.leLong(i) & d2.leLong(i)
        })
        dense
      case (d : DenseIoBits[IoId1], s : SparseIoBits[IoId2]) =>
        sparse.write(output, new SparseBits(s.trues.filter(d(_)).toSeq, s.n))
        sparse
      case (s : SparseIoBits[IoId1], d : DenseIoBits[IoId2]) =>
        sparse.write(output, new SparseBits(s.trues.filter(d(_)).toSeq, s.n))
        sparse
      case (s1 : SparseIoBits[IoId1], s2 : SparseIoBits[IoId2]) =>
        val trues = ArrayBuffer[Long]()
        var i = 0L
        var j = 0L
        while (i < s1.trues.size && j < s2.trues.size) {
          val v1 = s1.trues(i)
          val v2 = s2.trues(j)
          if (v1 == v2) {
            trues += v1
            i += 1
            j += 1
          } else if (v1 < v2) {
            i += 1
          } else {
             j+= 1
          }
        }
        sparse.write(output, new SparseBits(trues, s1.n))
        sparse
      case (s1 : EmptyIoBits[IoId1], _) =>
        sparse.write(output, new SparseBits(Seq(), s1.n))
        sparse
      case (_, s2 : EmptyIoBits[_]) =>
        sparse.write(output, new SparseBits(Seq(), s2.n))
        sparse
    }*/
  }
  def createAnd[IoId1, IoId2](file: FileRef[IoId], b1:IoBits[IoId1], b2:IoBits[IoId2]) : IoBits[IoId] = {
    val typ = using( file.openOutput) { output =>
      writeAnd(new DataOutputStream(output), b1, b2)
    }
    using(file.open) { typ.open(_) }
  }
  def createAnd[IoId1, IoId2](dir: Dir[IoId], b1:IoBits[IoId1], b2:IoBits[IoId2]) : IoBits[IoId] = {
    createAnd(FileRef(dir, dir.freeId), b1, b2)
  }

  def writeAndNot[IoId1, IoId2](output: DataOutputStream, b1:IoBits[IoId1], b2:IoBits[IoId2]) : SeqIoType[IoId, _ <: IoBits[IoId], Boolean] = {
    if (b1.n != b2.n) throw new IllegalArgumentException()
    (IoBits.isDense(b1), IoBits.isDense(b2)) match {
      case (true, true) =>
        dense.write(output, b1.size,
          new Iterable[Long] {
            def iterator = new Iterator[Long] {
              val i = b1.leLongs.iterator
              val j = b2.leLongs.iterator

              def hasNext = i.hasNext

              def next = i.next & ~j.next
            }
          })

        dense
      case (true, false) =>
        var trues = PeekIterator(b2.trues.iterator)
        dense.write(output, b1.size, (b1.leLongs zipWithIndex).map { case (l, i) =>
          var rv = l
          val begin = i*64
          val end = begin+64
          while (trues.hasNext && trues.head < end) {
            rv &= ~(1<<(trues.head-begin))
            trues.next
          }
          rv
        })
        dense
      case (false, true) =>
        sparse.write(output, new SparseBits(b1.trues.filter(!b2(_)).toSeq, b1.n))
        sparse
      case (false, false) =>
        val trues = ArrayBuffer[Long]()
        var i = PeekIterator[Long](b1.trues.iterator)
        var j = PeekIterator[Long](b2.trues.iterator)
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
        sparse.write(output, new SparseBits(trues, b1.n))
        sparse
    }
  }
  def createAndNot[IoId1, IoId2](file: FileRef[IoId], b1:IoBits[IoId1], b2:IoBits[IoId2]) : IoBits[IoId] = {
    val typ = using( file.openOutput) { output =>
      writeAndNot(new DataOutputStream(output), b1, b2)
    }
    using(file.open) { typ.open(_) }
  }
  def createAndNot[IoId1, IoId2](dir: Dir[IoId], b1:IoBits[IoId1], b2:IoBits[IoId2]) : IoBits[IoId] = {
    createAndNot(FileRef(dir, dir.freeId), b1, b2)
  }

  def writeNot[IoId1, IoId2](output: DataOutputStream, b:IoBits[IoId1]) : SeqIoType[IoId, _ <: IoBits[IoId], Boolean] = {
    unwrap(b) match {
      case d : DenseIoBits[IoId1] =>
        // TODO: decide between dense & sparse based on frequences
        dense.write(output, d.size, (0L until d.longCount).map { i =>
          ~d.leLong(i)
        })
        dense
      case s : SparseIoBits[IoId1] =>
        var at = 0
        dense.write(output, s.size, (0L until DenseIoBits.bitsToLongCount(s.size)).map { i =>
          var l = ~0L
          val begin = i*64
          val end = begin+64
          while (at < s.trues.size && s.trues(at) < end) {
            l &= ~((1L<<(s.trues(at)-begin))&0xffffffff)
            at+=1
          }
          l
        })
        dense
      case e : EmptyIoBits[IoId1] =>
        dense.write(output, e.size, (0L until DenseIoBits.bitsToLongCount(e.size)).map { i =>
          ~0L
        })
        dense

    }
  }
  def createNot[IoId1, IoId2](file: FileRef[IoId], b:IoBits[IoId1]) : IoBits[IoId] = {
    val typ = using( file.openOutput) { output =>
      writeNot(new DataOutputStream(output), b)
    }
    using(file.open) { typ.open(_) }
  }
  def createNot[IoId1, IoId2](dir: Dir[IoId], b:IoBits[IoId1]) : IoBits[IoId] = {
    createNot(FileRef(dir, dir.freeId), b)
  }

  def createMerged[IoId1, IoId2](file: FileRef[IoId], b1:IoBits[IoId1], b2:IoBits[IoId2]) : IoBits[IoId] = {
    using( file.openOutput) { output =>
      writeMerged2(new DataOutputStream(output), b1, b2)
    }
    using(file.open) { open(_) }
  }
  def createMerged[IoId1, IoId2](dir: Dir[IoId], b1:IoBits[IoId1], b2:IoBits[IoId2]) : IoBits[IoId] = {
    createMerged(FileRef(dir, dir.freeId), b1, b2)
  }

}

