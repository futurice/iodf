package com.futurice.iodf.utils

import java.util

/**
  * Created by arau on 31.5.2017.
  */
trait Bits {
  def trues : Iterable[Long]
  def lsize : Long
  def f : Long
  def n = lsize
}

case class SparseBits(trues:Iterable[Long], lsize:Long) extends Bits {
  def f = trues.size
}

case class DenseBits(bits:util.BitSet, lsize:Long) extends Bits {
  def f = bits.cardinality()
  def trues = new Iterable[Long]{
    override def iterator: Iterator[Long] = {
      new Iterator[Long] {
        var n = bits.nextSetBit(0)
        def hasNext = n >= 0 && n < size
        def next = {
          val rv = n
          n = bits.nextSetBit(n+1)
          rv
        }
      }
    }
  }
}

