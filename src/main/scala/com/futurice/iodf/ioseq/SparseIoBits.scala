package com.futurice.iodf.ioseq

import java.io.DataOutputStream
import java.util

import com.futurice.iodf.Utils._
import com.futurice.iodf._
import com.futurice.iodf.store.{FileRef, IoData}
import com.futurice.iodf.utils._

import scala.reflect.runtime.universe._

class SparseIoBitsType[Id](implicit val t:TypeTag[LBits])
  extends IoTypeOf[Id, SparseIoBits[Id], LBits]()(t)
    with IoSeqType[Id, Boolean, LBits, SparseIoBits[Id]] {
  val longs = new LongIoArrayType[Id]()

  override def defaultSeq(lsize:Long) = Some(LBits.empty(lsize))

  def write(output: DataOutputStream, f:Long, n:Long, trues: Iterator[Long]): Unit = {
    output.writeLong(n)
    longs.write(output, f, trues)
  }
  override def write(output: DataOutputStream, v: LBits): Unit = {
    write(output, v.f, v.n, v.trues.iterator)
  }
  override def writeSeq(output: DataOutputStream, v: LBits): Unit = {
    write(output, v)
  }
  def viewMerged(seqs:Seq[LBits]): LBits = {
    MultiBits(seqs)
  }
  override def writeMerged(out: DataOutputStream, seqs:Seq[LBits]): Unit = {
    var at = 0L
    val begins = seqs.map { s =>
      val rv = at
      at += s.lsize
      rv
    }

    out.writeLong(at)

    longs.writeMerged2(
      out,
      (seqs zip begins).map { case (seq, begin) =>
        (seq.lsize, seq.trues.iterator.map(_ + begin))
      })
  }
  override def open(buf: IoData[Id]) = {
    using(buf.openRandomAccess) { ra =>
      val sz = ra.getBeLong(0)
      using (buf.openView(8)) { view =>
        new SparseIoBits[Id](
          IoRef(this, buf.ref),
          longs.open(view).asInstanceOf[LongIoArray[Id]],
          sz)
      }
    }
  }

  override def valueTypeTag =
    _root_.scala.reflect.runtime.universe.typeTag[Boolean]

}

/**
  * Created by arau on 15.12.2016.
  */
class SparseIoBits[IoId](val ref:IoRef[IoId, SparseIoBits[IoId]],
                         val indexes:LongIoArray[IoId],
                         val lsize : Long) extends IoBits[IoId]{
  override def apply(l: Long): Boolean = {
    Utils.binarySearch(indexes, l)._1 != -1
  }
  override def close(): Unit = {
    indexes.close
  }
  override def f: Long = {
    indexes.size
  }
  override def fAnd(bits : LBits): Long = {
    bits match {
      case b : WrappedIoBits[_] => fAnd(b.unwrap)
      case b : SparseIoBits[_] => fAnd(b)
      case b : DenseIoBits[_] => IoBits.fAndDenseSparse(b, this)
      case _ =>
        LBits.fAnd(this, bits)
    }
  }
  def fAnd(b : SparseIoBits[_]): Long = fAndSparse(b)

  private def truesFrom(from:Long) : Scanner[Long, Long] = {
    new Scanner[Long, Long] {
      var at = 0L
      def hasNext = at < indexes.lsize
      def copy = truesFrom(from)
      override def headOption =
        hasNext match {
          case true => Some(head)
          case false => None
        }
      override def head : Long = indexes.apply(at)
      def seek(target:Long) = {
        val (hit, low, high) =
          Utils.binarySearch(indexes, target, at, at + target - head)
        at = high
        hit != -1
      }
      def next = {
        val rv = head
        at += 1
        rv
      }
    }
  }

  def trues = new Scannable[Long, Long] {
    def iterator = truesFrom(0)
  }

  def fAndSparse(b : SparseIoBits[_]): Long = {
    var rv = 0L
    var i = 0L
    var j = 0L
    val t1 = indexes.size
    val t2 = b.indexes.size
    while (i < t1 && j < t2) {
      val t1 = indexes(i)
      val t2 = b.indexes(j)
      if (t1 < t2) i += 1
      else if (t1 > t2) j += 1
      else {
        rv += 1
        i += 1
        j += 1
      }
    }
    rv
  }
}
