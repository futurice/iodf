package com.futurice

/**
  * Created by arau on 13.7.2017.
  */
package object iodf {

  def using[T <: AutoCloseable, E](d:T)(f : T => E) = {
    try {
      f(d)
    } finally {
      d.close
    }
  }
  def scoped[E](f : IoScope => E) = {
    using(IoScope.open) { scope => f(scope) }
  }

}
