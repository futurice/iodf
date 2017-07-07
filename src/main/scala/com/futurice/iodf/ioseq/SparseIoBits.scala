package com.futurice.iodf.ioseq

import java.io.DataOutputStream
import java.util

import com.futurice.iodf.Utils._
import com.futurice.iodf._
import com.futurice.iodf.io.{DataAccess, DataOutput, DataRef, IoRef}
import com.futurice.iodf.store.FileRef
import com.futurice.iodf.util._

import scala.reflect.runtime.universe._

class SparseIoBitsType
  extends SeqIoType[Boolean, LBits, SparseIoBits] {
  val longs = new LongIoArrayType()

  override def interfaceType = typeOf[LBits]
  override def ioInstanceType = typeOf[SparseIoBits]
  override def valueTypeTag = implicitly[TypeTag[Boolean]]

  override def defaultInstance(lsize:Long) = Some(LBits.empty(lsize))

  def write(output: DataOutput, f:Long, n:Long, trues: Iterator[Long]) : Unit = {
    output.writeLong(n)
    longs.write(output, f, trues)
  }
  override def write(output: DataOutput, v: LBits) : Unit = {
    write(output, v.f, v.n, v.trues.iterator)
  }
  def viewMerged(seqs:Seq[Ref[LBits]]) : LBits = {
    MultiBits.open(seqs)
  }
/*  override def writeMerged(out: DataOutputStream, seqs:Seq[LBits]): Unit = {
    var at = 0L
    val begins = seqs.map { s =>
      val rv = at
      at += s.f
      rv
    }

    out.writeLong(seqs.map(_.f))

    longs.writeMerged2(
      out,
      (seqs zip begins).map { case (seq, begin) =>
        (seq.lsize, seq.trues.iterator.map(_ + begin))
      })
  }*/

  override def open(data: DataAccess) = {
    val sz = data.getBeLong(0)
    using (data.openView(8, data.size)) { view =>
      new SparseIoBits(
        IoRef.open(this, data.dataRef),
        longs.open(view).asInstanceOf[LongIoArray],
        sz)
    }
  }

}

/**
  * Created by arau on 15.12.2016.
  */
class SparseIoBits(val _openRef:IoRef[SparseIoBits],
                   val indexes:LongIoArray,
                   val lsize : Long) extends IoBits {
  override def openRef = _openRef.openCopy
  override def apply(l: Long): Boolean = {
    val e = Utils.binarySearch(indexes, l, 0, l+1)
    e._1 != -1
  }
  override def close(): Unit = {
    _openRef.close
    indexes.close
  }
  override def f: Long = {
    indexes.lsize
  }
  override def fAnd(bits : LBits): Long = {
    bits match {
      case b : WrappedIoBits => fAnd(b.unwrap)
      case b : SparseIoBits => fAnd(b)
      case b : DenseIoBits => IoBits.fAndDenseSparse(b, this)
      case _ =>
        LBits.fAnd(this, bits)
    }
  }
  override def isDense = false
  def fAnd(b : SparseIoBits): Long = fAndSparse(b)

  private def truesFromTrue(from:Long) : Scanner[Long, Long] = {
    new Scanner[Long, Long] {
      var at = from
      def hasNext = at < indexes.lsize
      def copy = truesFromTrue(at)
      override def headOption =
        hasNext match {
          case true => Some(head)
          case false => None
        }
      override def head : Long = indexes.apply(at)
      def seek(target:Long) = {
        at = indexAfter(at, target)
/*        val (hit, low, high) =
          Utils.binarySearch(indexes, target, at, at + target - head + 1)
        at = high*/
        at < indexes.lsize && indexes(at) == target
      }
      def next = {
        val rv = head
        at += 1
        rv
      }
    }
  }

  def trues = new Scannable[Long, Long] {
    def iterator = truesFromTrue(0)
  }

  def indexAfter(from:Long, target:Long) = {
    var i = from
    val max = indexes.lsize
    var jump = 8
    while (i+jump < max && indexes(i+jump) <= target) {
      i = i+jump
      jump *= 2
    }
    while (i < max && indexes(i) < target) i += 1
    i
  }

  def fAndSparse(b : SparseIoBits): Long = {
    var rv = 0L
    var i = 0L
    var j = 0L
    val max1 = indexes.size
    val max2 = b.indexes.size
    while (i < max1 && j < max2) {
      val t1 = indexes(i)
      val t2 = b.indexes(j)
      if (t1 < t2) {
        i = indexAfter(i+1, t2)
/*        i += 1
        while (i < max1 && indexes(i) < t2) i += 1*/

      } else if (t2 < t1) {
        j = b.indexAfter(j+1, t1)
/*        j += 1
        while (j < max2 && b.indexes(j) < t1) j += 1*/
      } else {
        rv += 1
        i += 1
        j += 1
      }
    }
    rv
  }
}
