package com.futurice.iodf.io

import com.futurice.iodf.{IoContext, IoScope}
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
  def openCopy   : DataRef

  def openView(from:Long, until:Long) : DataRef

  // Helper methods, should these be provided by implicit monads?

  def access(implicit bind:IoScope) = bind(openAccess)
  def openAs[T](implicit io:IoContext): T = {
    io.openAs[T](this)
  }
  def as[T](implicit io:IoContext, scope:IoScope): T = {
    io.as[T](this)
  }
}

object DataRef {
  def open(memRef:Ref[Memory], from:Long = 0, sz:Option[Long] = None) : DataRef = {
    new DataRef {
      val ref = memRef.openCopy
      val f = from
      override def openAccess: DataAccess =
        using (new DataAccess(this, ref)) { ref =>
          ref.openView(from, from + sz.getOrElse(ref.size - from))
        }
      def openCopy = DataRef.open(ref, from, sz)
      override def byteSize: Long =
        sz.getOrElse(ref.get.size())

      override def openView(from: Long, until: Long): DataRef =
        DataRef.open(ref, f + from, Some(until))

      override def close(): Unit = ref.close
    }
  }

}