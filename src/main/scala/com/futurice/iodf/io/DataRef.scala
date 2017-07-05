package com.futurice.iodf.io

import com.futurice.iodf.util.{Handle, Ref}
import com.futurice.iodf.Utils.using
import xerial.larray.buffer.Memory

/**
  * Why reference need to be opened, copied and closed?
  *
  *   Because otherwise we couldn't release ad-hoc allocated resources
  *   in off-heap memory, in filesystem, or in remote hosts!
  *
  *   We have to control the resource life cycles.
  *
  */
trait DataRef extends Handle {
  def openAccess : DataAccess
  def byteSize   : Long
  def copy       : DataRef

  def openView(from:Long, until:Long) : DataRef
}

object DataRef {
  def open(memRef:Ref[Memory], from:Long = 0, sz:Option[Long] = None) : DataRef = {
    new DataRef {
      val ref = memRef.copy
      val f = from
      override def openAccess: DataAccess =
        using (new DataAccess(this, ref)) { ref =>
          ref.openView(from, from + sz.getOrElse(ref.size - from))
        }
      def copy = DataRef.open(memRef, from, sz)
      override def byteSize: Long =
        sz.getOrElse(ref.get.size())

      override def openView(from: Long, until: Long): DataRef =
        DataRef.open(memRef, f + from, Some(until))

      override def close(): Unit = memRef.close
    }
  }

}