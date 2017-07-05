package com.futurice.iodf.io

import java.io.OutputStream


/**
  * This is basically a more sophisticated output stream for writing data
  * One the key thigns in here, is that you can random access the already written data.
  */
trait DataOutput extends OutputStream with java.io.DataOutput {

  def pos : Long

  /**
   * this provides ability to access previously written data, while it is being written
   * the data reference is guaranteed to begin from the begin of created memory area,
   * and end exactly at the point, or somewhere after this data was being written.
   */
  def openDataRef : DataRef
  /* closes and finalizes the data, and returns an open data reference */
  def adoptResult : DataRef

  def writeVInt(l:Int) : Unit

  def writeVLong(l:Long) : Unit

  def writeLeLong(l:Long) : Unit

}
