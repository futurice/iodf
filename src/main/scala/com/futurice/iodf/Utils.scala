package com.futurice.iodf

import scala.annotation.tailrec

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
