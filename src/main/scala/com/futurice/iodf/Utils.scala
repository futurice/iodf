package com.futurice.iodf

import java.io.Closeable

import com.futurice.iodf.ioseq.{DenseIoBitsType, IoBits, IoBitsType, SparseIoBitsType}
import com.futurice.iodf.store.RamDir

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

class IoScope(val io:IoUtils) extends Closeable {
  val closeables = ArrayBuffer[Closeable]()
  override def close(): Unit = {
    closeables.foreach(_.close)
    closeables.clear()
  }
  def bind(c:Closeable) = {
    closeables += c
    c
  }
}


class IoUtils extends Closeable {
  val ram = RamDir()
  val bits =
    new IoBitsType(new SparseIoBitsType[Int](),
                   new DenseIoBitsType[Int]())
  override def close(): Unit = {
    ram.close()
  }
  def openScope = new IoScope(this)
  def and[IoId1, IoId2](b1:IoBits[IoId1], b2:IoBits[IoId2])(implicit scope:IoScope) = {
    scope.bind(bits.createAnd(scope.io.ram, b1, b2))
  }
  def not[IoId1, IoId2](b:IoBits[IoId1])(implicit scope:IoScope)  = {
    scope.bind(bits.createNot(ram, b))
  }
}

/**
  * Created by arau on 24.11.2016.
  */
object Utils {

  def binarySearch[T](sortedSeq:IoSeq[_, T], target:T)(implicit ord:Ordering[T]) = {
    @tailrec
    def recursion(low:Int, high:Int):Int = (low+high)/2 match{
      case _ if high < low => -1
      case mid if ord.gt(sortedSeq(mid), target) => recursion(low, mid-1)
      case mid if ord.lt(sortedSeq(mid), target) => recursion(mid+1, high)
      case mid => mid
    }
    recursion(0, sortedSeq.size - 1)
  }

  def using[T <: AutoCloseable, E](d:T)(f : T => E) = {
    try {
      f(d)
    } finally {
      d.close
    }
  }

}
