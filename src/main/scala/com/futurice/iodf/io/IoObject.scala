package com.futurice.iodf.io

import java.io.Closeable

import com.futurice.iodf.Utils._

/**
 * This object is io-referable
 */
trait IoObject extends Closeable {
  def openRef : IoRef[_ <: IoObject]

  def ioType : IoOpener[_ <: IoObject] = using (openRef) { _.typ }
}

/**
  * This is an object, which pretends to be another object. This is used
  * for adding closed io objects to sequences
  */
case class IoRefObject[T <: IoObject](ref:IoRef[_ <: T])
  extends IoObject {
  def openRef = ref.openCopy
  override def close(): Unit = {}
}