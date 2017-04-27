package com.futurice.iodf

import java.io.Closeable

trait IoObject[Id] extends Closeable {
  def ref : IoRef[Id, _ <: IoObject[Id]]
}

/**
  * This is an object, which pretends to be another object. This is used
  * for adding closed io objects to sequences
  */
case class IoRefObject[Id, T <: IoObject[Id]](override val ref:IoRef[Id, _ <: T]) extends IoObject[Id] {
  override def close(): Unit = {}
}