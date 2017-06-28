package com.futurice.iodf.io

import java.io.Closeable

/**
 * This object is io-referable
 */
trait IoObject extends Closeable {
  def openRef : IoRef[_ <: IoObject]
}

/**
  * This is an object, which pretends to be another object. This is used
  * for adding closed io objects to sequences
  */
case class IoRefObject[T <: IoObject](ref:IoRef[_ <: T])
  extends IoObject {
  def openRef = ref.copy
  override def close(): Unit = {}
}