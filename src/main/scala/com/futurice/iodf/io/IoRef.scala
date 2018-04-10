package com.futurice.iodf.io

import com.futurice.iodf.IoScope
import com.futurice.iodf.util.Handle
import com.futurice.iodf._

/**
  * Io reference refers to an io type
  */
case class IoRef[T](val typ:IoOpener[_ <: T], dataRef:DataRef) {
  def apply = typ.apply(dataRef.access)
}

