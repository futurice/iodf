package com.futurice.iodf.providers

import com.futurice.iodf.io.{MaxBound, MinBound}

import scala.reflect.runtime.universe._

/**
  * Created by arau on 5.7.2017.
  */
object OrderingProvider {

  def orderingOf[T](implicit ord: Ordering[T]) = ord

  // FIXME: copy paste code

  def orderingOf(tpe: Type) = {
    (tpe match {
      case t if t <:< typeOf[Boolean] => orderingOf[Boolean]
      case t if t <:< typeOf[Int] => orderingOf[Int]
      case t if t <:< typeOf[Long] => orderingOf[Long]
      case t if t <:< typeOf[String] => orderingOf[String]
      case t if t <:< typeOf[Option[Boolean]] => orderingOf[Option[Boolean]]
      case t if t <:< typeOf[Option[Int]] => orderingOf[Option[Int]]
      case t if t <:< typeOf[Option[Long]] => orderingOf[Option[Long]]
      case t if t <:< typeOf[Option[String]] => orderingOf[Option[String]]
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
        case ((a0, a1), (b0, b1)) =>
          compare(a0, b0) match {
            case 0 => compare(a1, b1)
            case i => i
          }
        case ((a0, a1, a2), (b0, b1, b2)) =>
          compare(a0, b0) match {
            case 0 =>
              compare(a1, b1) match {
                case 0 => compare(a2, b2)
                case i => i
              }
            case i => i
          }
        case (as:Array[_], bs:Array[_]) =>
          (as zip bs).iterator.map { case (a, b) => compare(a, b) }.find { _ != 0 }.getOrElse(0)
            match {
              case 0 => as.length compare bs.length
              case i => i
            }

        case (_ : MinBound, _) => -1
        case (_ : MaxBound, _) => 1
        case (_, _ : MinBound) => 1
        case (_, _ : MaxBound) => -1
        case (xv: Boolean, yv:Boolean) => b.compare(xv, yv)
        case (xv: Int, yv:Int) => i.compare(xv, yv)
        case (xv: Long, yv:Long) => l.compare(xv, yv)
        case (xv: String, yv:String) => s.compare(xv, yv)
        case (None, None) => 0
        case (None, Some(_)) => 1
        case (Some(_), None) => -1
        case (Some(xv), Some(yv)) => compare(xv, yv)
        case (xv, yv) =>
          s.compare(xv.getClass.getName, yv.getClass.getName)
      }
    }
  }

}
