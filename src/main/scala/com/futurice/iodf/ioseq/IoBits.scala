package com.futurice.iodf.ioseq

import java.io.DataOutputStream
import java.util

import com.futurice.iodf.Utils._
import com.futurice.iodf.store.{Dir, FileRef, IoData}
import com.futurice.iodf.{IoRef, IoSeq, IoTypeOf, SeqIoType}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by arau on 15.12.2016.
  */
abstract class IoBits[IoId] extends IoSeq[IoId, Boolean] {

  def n = lsize
  def f : Long
  def fAnd(bits:IoBits[_]) : Long

  def trues : Iterable[Long]

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
          d1.beLong(i) & d2.beLong(i)
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
}
