package com.futurice.iodf.io

import com.futurice.iodf.store.{DataRef, Handle}

/**
  * Io reference refers to an io type
  */
case class IoRef[T](typ:IoOpener[_ <: T], dataRef:DataRef) extends Handle {
  val d = dataRef.copy
  def open = typ.open(d)
  def copy = new IoRef[T](typ, d)
  def close: Unit = {
    d.close()
  }
}


