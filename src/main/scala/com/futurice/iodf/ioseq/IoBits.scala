package com.futurice.iodf.ioseq

import com.futurice.iodf.IoSeq

/**
  * Created by arau on 15.12.2016.
  */
abstract class IoBits[IoId] extends IoSeq[IoId, Boolean] {

  def n = lsize
  def f : Long
  def fAnd(bits:IoBits[IoId]) : Long

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
