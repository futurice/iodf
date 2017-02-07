package com.futurice.iodf.ioseq

import java.io.DataOutputStream
import java.util

import com.futurice.iodf.Utils._
import com.futurice.iodf._
import com.futurice.iodf.store.{FileRef, IoData}

import scala.reflect.runtime.universe._

case class SparseBits(trues:Seq[Long], size:Long) {}

class SparseToDenseIoBitsType[Id](implicit val t:TypeTag[SparseBits])
  extends IoTypeOf[Id, DenseIoBits[Id], SparseBits]()(t)
    with SeqIoType[Id, DenseIoBits[Id], Boolean] {

  override def write(output: DataOutputStream, v: SparseBits): Unit = {
    output.writeLong(v.size)
    var i = 0
    val bits = new util.BitSet(v.size.toInt)
    v.trues.foreach { l => bits.set(l.toInt) }
    val bytes = bits.toByteArray
    bytes.foreach { b =>
      output.writeByte(b)
    }
    // write trailing bytes
    val byteSize = DenseIoBits.bitsToByteSize(v.size)
    (bytes.size until byteSize.toInt).foreach { i => output.writeByte(0) }
  }

  override def open(buf: IoData[Id]) = {
    new DenseIoBits[Id](IoRef(this, buf.ref), buf.openRandomAccess)
  }
  override def valueTypeTag =
    _root_.scala.reflect.runtime.universe.typeTag[Boolean]
}


class SparseIoBitsType[Id](implicit val t:TypeTag[SparseBits])
  extends IoTypeOf[Id, SparseIoBits[Id], SparseBits]()(t)
    with SeqIoType[Id, SparseIoBits[Id], Boolean] {
  val longs = new LongIoArrayType[Id]()

  override def write(output: DataOutputStream, v: SparseBits): Unit = {
    output.writeLong(v.size)
    longs.write(output, v.trues)
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
  def fAnd(b : SparseIoBits[_]): Long = {
    var rv = 0
    var i = 0
    var j = 0
    while (i < trues.size && j < b.trues.size) {
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
