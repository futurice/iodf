package com.futurice.iodf.io

import com.futurice.iodf.IoScope
import com.futurice.iodf.util.Handle
import com.futurice.iodf.Utils._

/**
  * Io reference refers to an io type
  */
class IoRef[T](val typ:IoOpener[_ <: T], dataRef:DataRef) extends Handle {
  val d = dataRef.openCopy
  def open = using (d.openAccess)( typ.open )
  def openCopy = new IoRef[T](typ, d)
  def close: Unit = {
    d.close()
  }
}

object IoRef {
  def open[T](typ:IoOpener[_ <: T], dataRef:DataRef) : IoRef[T] =
    new IoRef[T](typ, dataRef)
  def apply[T](typ:IoOpener[_ <: T], dataRef:DataRef)(implicit bind:IoScope) : IoRef[T] =
    bind(open(typ, dataRef))
}