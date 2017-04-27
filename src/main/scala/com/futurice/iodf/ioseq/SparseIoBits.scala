package com.futurice.iodf.ioseq

import java.io.DataOutputStream
import java.util

import com.futurice.iodf.Utils._
import com.futurice.iodf._
import com.futurice.iodf.store.{FileRef, IoData}

import scala.reflect.runtime.universe._

case class SparseBits(trues:Iterable[Long], size:Long) {}

class SparseIoBitsType[Id](implicit val t:TypeTag[SparseBits])
  extends IoTypeOf[Id, SparseIoBits[Id], SparseBits]()(t)
    with SeqIoType[Id, SparseIoBits[Id], Boolean] {
  val longs = new LongIoArrayType[Id]()

  override def defaultSeq(lsize:Long) = {
    new EmptyIoBits[Id](lsize)
  }
  override def write(output: DataOutputStream, v: SparseBits): Unit = {
    output.writeLong(v.size)
    longs.write(output, v.trues.size, v.trues.iterator)
  }
  def writeMerged(out: DataOutputStream, seqA: SparseIoBits[Id], seqB: SparseIoBits[Id]): Unit = {
    out.writeLong(seqA.lsize+seqB.lsize)
    longs.writeMerged2(out, seqA.trues, seqB.trues.lsize, seqB.trues.iterator.map(_+seqA.lsize))
  }
  override def writeAnyMerged(out:DataOutputStream, seqA:Any, seqB:Any) = {
    (seqA, seqB) match {
      case (a:SparseIoBits[Id], b:SparseIoBits[Id]) =>
        writeMerged(out, a, b)
      case (a:SparseIoBits[Id], b:EmptyIoBits[Id]) =>
        out.writeLong(a.lsize+b.lsize)
        a.trues.buf.writeTo(0, out, a.trues.buf.size) // write copy
      case (a:EmptyIoBits[Id], b:SparseIoBits[Id]) =>
        out.writeLong(a.lsize+b.lsize)
        longs.write(out, b.trues.lsize, b.trues.iterator.map(_+a.lsize))
      case (a:SparseIoBits[Id], b:DenseIoBits[Id]) =>
        out.writeLong(a.lsize+b.lsize)
        if (b.trues.size != b.f) {
          throw new RuntimeException(f"FAILURE: ${b.trues.size} vs ${b.f}") // 18 vs 19
        }
        longs.writeMerged2(out, a.trues, b.f, b.trues.iterator.map(_+a.lsize))
    }
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
                         val trues:LongIoArray[IoId],
                         val lsize : Long) extends IoBits[IoId]{

  def beLongs = {
    val longs = DenseIoBits.bitsToLongCount(lsize)
    new Iterator[Long] {
      var at = 0L
      var i = 0L
      override def hasNext = i < longs
      override def next = {
        var rv = 0L
        val sz = trues.size
        while (at < sz && trues(at) < (i+1)*64) {
          // these BE longs are crazy :-/
          val offset = trues(at)-i*64
          val byte = 7-(offset / 8)
          val bit = offset%8
          rv |= (1L << bit) << (8*byte)
          at += 1
        }
        i += 1
        rv
      }
    }
  }
  def leLongs = {
    val longs = DenseIoBits.bitsToLongCount(lsize)
    new Iterator[Long] {
      var at = 0L
      var i = 0L
      override def hasNext = i < longs
      override def next = {
        var rv = 0L
        val sz = trues.size
        while (at < sz && trues(at) < (i+1)*64) {
          rv |= (1L << (trues(at)-i*64))
          at += 1
        }
        i += 1
        rv
      }
    }
  }
  override def apply(l: Long): Boolean = {
/*    var i = 0L
    while (i < trues.lsize && trues(i) < l) {
      i += 1
    }
    trues(i) == l */
    Utils.binarySearch(trues, l) != -1
  }
  override def close(): Unit = {
    trues.close
  }
  override def f: Long = {
    trues.size
  }
  override def fAnd(bits : IoBits[_]): Long = {
    bits match {
      case b : SparseIoBits[_] => fAnd(b)
      case b : DenseIoBits[_] => IoBits.fAnd(b, this)
      case b : EmptyIoBits[_] => 0
    }
  }
  def fAnd(b : SparseIoBits[_]): Long = fAndSparse(b)

  def fAndSparse(b : SparseIoBits[_]): Long = {
    var rv = 0L
    var i = 0L
    var j = 0L
    val t1 = trues.size
    val t2 = b.trues.size
    while (i < t1 && j < t2) {
      val t1 = trues(i)
      val t2 = b.trues(j)
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

object SparseIoBits {

  def create[IoId](trues:Iterable[Long], size:Long)(io:IoContext[IoId]) = {
    io.bits.createSparse(io.dir, trues, size)
  }
  def apply[IoId](trues:Iterable[Long], size:Long)(io:IoContext[IoId], scope:IoScope) = {
    scope.bind(io.bits.createSparse(io.dir, trues, size))
  }

}