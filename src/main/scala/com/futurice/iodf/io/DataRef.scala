package com.futurice.iodf.io

import com.futurice.iodf.{IoContext, IoScope}
import com.futurice.iodf.util.{AutoClosed, Handle, Ref}
import com.futurice.iodf._
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
trait DataRef {

  def access : DataAccess

  def byteSize   : Long

  def view(from:Long, until:Long) : DataRef

  def as[T](implicit io:IoContext, scope:IoScope): T = {
    io.as[T](this)
  }
}

object DataRef {
  def apply(memRef:AutoClosed[Memory], from:Long = 0, sz:Option[Long] = None) : DataRef = {
    new DataRef {
      val f = from
      override def access: DataAccess =
        new DataAccess(this, memRef, from, sz.map(from + _))
      def copy = DataRef(memRef, from, sz)
      override def byteSize: Long =
        sz.getOrElse(memRef.get.size())

      override def view(from: Long, until: Long): DataRef =
        DataRef(memRef, f + from, Some(until))
    }
  }

}