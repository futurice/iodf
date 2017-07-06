package com.futurice.iodf.store

import com.futurice.iodf.io.{DataOutput, DataOutputMixin, DataRef}
import com.futurice.iodf.util.Ref
import com.futurice.iodf.Utils._
import xerial.larray.buffer.LBuffer

/**
  * Created by arau on 5.7.2017.
  */
class LBufferCreator extends DataOutput with DataOutputMixin {
  var pos = 0L
  var ref : Option[Ref[LBuffer]] = None
  def buf = ref.get.get

  increaseBuffer(LBufferCreator.defaultBufferSize)

  def increaseBuffer(size:Long) = {
    val m = new LBuffer(size)
    ref.foreach { r =>
      r.get.copyTo(0, m, 0, pos)
      r.close
    }
    ref = Some(Ref.open[LBuffer](m, () => m.release()))
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
    ref.foreach { r =>
      r.close
    }
    ref = None
  }
  override def openDataRef: DataRef = {
    using (ref.get.openMapped(_.m)) { ref =>
      DataRef.open(ref, 0, Some(pos))
    }
  }
}

object LBufferCreator {
  val defaultBufferSize = 1024
}