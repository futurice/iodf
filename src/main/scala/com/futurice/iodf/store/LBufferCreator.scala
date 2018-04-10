package com.futurice.iodf.store

import com.futurice.iodf.io.{DataOutput, DataOutputMixin, DataRef}
import com.futurice.iodf.util.{AutoClosed, Ref}
import com.futurice.iodf.Utils._
import com.futurice.iodf._
import xerial.larray.buffer.LBuffer

/**
  * Created by arau on 5.7.2017.
  */
class LBufferCreator(implicit io:IoContext) extends DataOutput with DataOutputMixin {
  var pos = 0L
  var ref : Option[AutoClosed[LBuffer]] = None
  def buf = ref.get.get

  increaseBuffer(LBufferCreator.defaultBufferSize)

  def increaseBuffer(size:Long) = {
    val m = new LBuffer(size)
    ref.foreach { r =>
      r.get.copyTo(0, m, 0, pos)
      // FIXME: could this buffer be closed right away?
    }
    ref = Some(AutoClosed(m)(_.release()))
  }

  override def write(b: Int): Unit = {
    if (pos == buf.size()) {
      increaseBuffer(buf.size*2)
    }
    buf.putByte(pos, b.toByte)
    pos += 1
  }
  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    while (pos + len >= buf.size()) {
      increaseBuffer(buf.size*2)
    }
    //public Int readFrom(src: Array[Byte], srcOffset: Int, destOffset: Long, length: Int)
    buf.readFrom(b, off, pos, len)
    pos += len
  }
  override def close = {
    ref = None
  }
  override def dataRef: DataRef = {
    DataRef(ref.get, 0, Some(pos))
  }
}

object LBufferCreator {
  val defaultBufferSize = 1024
}