package com.futurice.iodf.io

import java.io.Closeable

import com.futurice.iodf._

/**
 * This object is io-referable
 */
trait IoObject extends Closeable {
  def ref : IoRef[_ <: IoObject]

  def ioType : IoOpener[_ <: IoObject] = ref.typ
}

/**
  * This is an object, which pretends to be another object. This is used
  * for adding closed io objects to sequences
  */
case class IoRefObject[T <: IoObject](ref:IoRef[_ <: T])
  extends IoObject {
  override def close(): Unit = {}
}