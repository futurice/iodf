package com.futurice.iodf.providers

import com.futurice.iodf.io.{MaxBound, MinBound}

import scala.reflect.runtime.universe._

/**
  * Created by arau on 5.7.2017.
  */
object OrderingProvider {

  def orderingOf[T](implicit ord: Ordering[T]) = ord

  def orderingOf(tpe: Type) = {
    (tpe match {

      case t if t <:< typeOf[Boolean] => orderingOf[Boolean]
      case t if t <:< typeOf[Int] => orderingOf[Int]
      case t if t <:< typeOf[Long] => orderingOf[Long]
      case t if t <:< typeOf[String] => orderingOf[String]
      case v =>
        throw new IllegalArgumentException(v + " is unknown")
    }).asInstanceOf[Ordering[Any]] // TODO: refactor, there has to be a better way
  }
  def anyOrdering : Ordering[Any] = new Ordering[Any] {
    val b = orderingOf[Boolean]
    val i = orderingOf[Int]
    val l = orderingOf[Long]
    val s = orderingOf[String]
    override def compare(x: Any, y: Any): Int = {
      (x, y) match {
        case (_ : MinBound, _) => -1
        case (_ : MaxBound, _) => 1
        case (_, _ : MinBound) => 1
        case (_, _ : MaxBound) => -1
        case (xv: Boolean, yv:Boolean) => b.compare(xv, yv)
        case (xv: Int, yv:Int) => i.compare(xv, yv)
        case (xv: Long, yv:Long) => l.compare(xv, yv)
        case (xv: String, yv:String) => s.compare(xv, yv)
        case (x, y) =>
          throw new IllegalArgumentException("cannot compare " + x + " with " + y)
      }
    }
  }

}
