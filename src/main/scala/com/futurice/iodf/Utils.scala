package com.futurice.iodf

import java.io.Closeable

import com.futurice.iodf.ioseq.{DenseIoBitsType, IoBits, IoBitsType, SparseIoBitsType}
import com.futurice.iodf.store.{Dir, RamDir}

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

class IoScope extends Closeable{
  val closeables = ArrayBuffer[Closeable]()
  override def close(): Unit = {
    closeables.foreach(_.close)
    closeables.clear()
  }
  def bind[T <: Closeable](c:T) = {
    closeables += c
    c
  }
  def apply[T <: Closeable](c:T) : T = bind[T](c)
  def openScope = bind(new IoScope)

}

object IoScope {
  def open : IoScope = new IoScope()
}

class IoContext[IoId](openedDir:Dir[IoId]) extends Closeable {
  val dir = openedDir

  val bits =
    new IoBitsType(new SparseIoBitsType[IoId](),
      new DenseIoBitsType[IoId]())

  override def close(): Unit = {
    dir.close()
  }
}

object IoContext {
  def open = {
    new IoContext[Int](RamDir())
  }
  def apply()(implicit scope:IoScope) = {
    scope.bind(open)
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
