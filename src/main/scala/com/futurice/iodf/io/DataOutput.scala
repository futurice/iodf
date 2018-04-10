package com.futurice.iodf.io

import java.io.OutputStream

import com.futurice.iodf._

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
  def dataRef : DataRef

  def writeVInt(l:Int) : Unit

  def writeVLong(l:Long) : Unit

  def writeLeLong(l:Long) : Unit

  /* sub output has different positioning scheme*/
  def subOutput : DataOutput = {
    val self = this
    val myBegin = pos
    new DataOutput with DataOutputMixin {

      override def pos: Long = self.pos - myBegin

      /**
        * this provides ability to access previously written data, while it is being written
        * the data reference is guaranteed to begin from the begin of created memory area,
        * and end exactly at the point, or somewhere after this data was being written.
        */
      override def dataRef: DataRef =
        self.dataRef.view(myBegin, self.pos)

      override def write(b: Int): Unit = {
        self.write(b)
      }
      override def write(bytes:Array[Byte]) : Unit = {
        self.write(bytes)
      }
      override def write(bytes:Array[Byte], off:Int, len:Int) : Unit = {
        self.write(bytes, off, len)
      }
    }
  }

}
