package com.futurice.iodf.utils

import java.util

/**
  * Created by arau on 31.5.2017.
  */
trait Bits extends Iterable[Boolean] {
  def trues : Iterable[Long]
  def lsize : Long
  def f : Long
  def n = lsize
}

case class SparseBits(trues:Iterable[Long], lsize:Long) extends Bits {
  def f = trues.size
  def iterator = new Iterator[Boolean] {
    var i = 0;
    var t = PeekIterator(trues.iterator)
    def hasNext = i < lsize
    def next = {
      if (t.hasNext && t.head == i) {
        t.next
        i += 1
        true
      } else {
        i += 1
        false
      }
    }
  }
}

case class DenseBits(bits:util.BitSet, lsize:Long) extends Bits {
  def f = bits.cardinality()
  def iterator = new Iterator[Boolean] {
    var i = 0;
    def hasNext = i < lsize
    def next = {
      val rv = bits.get(i)
      i += 1
      rv
    }
  }
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

