package com.futurice.iodf.ioseq

import java.io.DataOutputStream
import java.util

import com.futurice.iodf.Utils._
import com.futurice.iodf.store.{Dir, FileRef, IoData}
import com.futurice.iodf._

import scala.collection.mutable.ArrayBuffer

/**
  * Created by arau on 15.12.2016.
  */
abstract class IoBits[IoId] extends IoSeq[IoId, Boolean] {

  def n = lsize
  def f : Long
  def fAnd(bits:IoBits[_]) : Long

  def trues : Iterable[Long]

  implicit def &[IoId1, IoId2](b:IoBits[IoId1])(implicit io:IoContext[IoId2], scope:IoScope) = {
    scope.bind(io.bits.createAnd(io.dir, this, b))
  }
  implicit def ~[IoId1](implicit io:IoContext[IoId1], scope:IoScope) = {
    scope.bind(io.bits.createNot(io.dir, this))
  }

}

class EmptyIoBits[IoId](val lsize : Long) extends IoBits[IoId] {
  override def f: Long = 0
  override def fAnd(bits: IoBits[_]): Long = 0
  override def trues: Iterable[Long] = Seq[Long]()
  override def apply(l: Long): Boolean = false
  override def ref: IoRef[IoId, _ <: IoObject[IoId]] =
    throw new RuntimeException("not referable")
  override def close(): Unit = {}
}

object IoBits {
  def fAnd(dense:DenseIoBits[_], sparse: SparseIoBits[_]) = {
    var rv = 0L
    sparse.trues.foreach { t =>
      if (dense(t)) rv += 1
    }
    rv
  }
}

class IoBitsType[IoId](val sparse:SparseIoBitsType[IoId], val dense:DenseIoBitsType[IoId], sparsityThreshold:Long = 4*1024) {

  def writeAnd[IoId1, IoId2](output: DataOutputStream, b1:IoBits[IoId1], b2:IoBits[IoId2]) : SeqIoType[IoId, _ <: IoBits[IoId], Boolean] = {
    if (b1.n != b2.n) throw new IllegalArgumentException()
    (b1, b2) match {
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
      case (_, s2 : EmptyIoBits[IoId1]) =>
        sparse.write(output, new SparseBits(Seq(), s2.n))
        sparse
    }
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
    (b1, b2) match {
      case (d1 : DenseIoBits[IoId1], d2 : DenseIoBits[IoId2]) =>
        dense.write(output, d1.size, (0L until d1.longCount).map { i =>
          d1.leLong(i) & ~d2.leLong(i)
        })
        dense
      case (d : DenseIoBits[IoId1], s : SparseIoBits[IoId2]) =>
        var at = 0
        dense.write(output, d.size, (0L until d.longCount).map { i =>
          var l = d.leLong(i)
          val begin = i*64
          val end = begin+64
          while (at < s.trues.size && s.trues(at) < end) {
            l &= ~(1<<(s.trues(at)-begin))
            at+=1
          }
          l
        })
        dense
      case (s : SparseIoBits[IoId1], d : DenseIoBits[IoId2]) =>
        sparse.write(output, new SparseBits(s.trues.filter(!d(_)).toSeq, s.n))
        sparse
      case (s1 : SparseIoBits[IoId1], s2 : SparseIoBits[IoId2]) =>
        val trues = ArrayBuffer[Long]()
        var i = 0L
        var j = 0L
        while (i < s1.trues.size && j < s2.trues.size) {
          val v1 = s1.trues(i)
          val v2 = s2.trues(j)
          if (v1 == v2) {
            i += 1
            j += 1
          } else if (v1 < v2) {
            trues += v1
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
/*      case (_, s2 : EmptyIoBits[IoId1]) =>
TODO
 */
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
    b match {
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
            l &= ~(1<<(s.trues(at)-begin))
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

}

