package com.futurice.iodf.store

import java.io.{Closeable, OutputStream}

import org.slf4j.LoggerFactory
import xerial.larray.buffer.{LBuffer, LBufferConfig}

import scala.collection.mutable
import scala.reflect.ClassTag

trait DataRef extends Closeable {
  def open : RandomAccess
  def size : Long
  def openView(from:Long, until:Option[Long]) : DataRef
}

trait DataCreator extends OutputStream {
  /**
    * Grabs the ownership of the resulting data
    */
  def adoptOpenResult : DataRef
}


/**
  * Created by arau on 26.6.2017.
  */
trait DataLand extends Closeable {
  def create : DataCreator
  def byteSize : Long
  def size : Long
}

object LBufferCreator {
  val defaultBufferSize = 1024
}

object DataRef {
  def open(memRef:RefCounted[MemoryResource], from:Long = 0, sz:Option[Long] = None) : DataRef =
    new DataRef {
      memRef.inc
      override def open: RandomAccess =
        new RandomAccess(memRef)

      override def size: Long = sz.getOrElse(memRef.value.memory.size())

      override def openView(f: Long, until: Option[Long]): DataRef =
        DataRef.open(memRef, from + f, until.orElse(sz))

      override def close(): Unit = memRef.close
    }

}

class LBufferCreator extends DataCreator {
  var pos = 0L
  var buf = new LBuffer(LBufferCreator.defaultBufferSize) // TODO: turn into option

  override def write(b: Int): Unit = {
    if (pos == buf.size()) {
      val m = new LBuffer(buf.size*2)
      buf.copyTo(0, m, 0, pos)
      buf.release()
      buf = m
    }
    buf.putByte(pos, b.toByte)
    pos += 1
  }
  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    while (pos + len >= buf.size()) {
      val m = new LBuffer(buf.size*2)
      buf.copyTo(0, m, 0, pos)
      buf.release()
      buf = m
    }
    //public Int readFrom(src: Array[Byte], srcOffset: Int, destOffset: Long, length: Int)
    buf.readFrom(b, off, pos, len)
    pos += len
  }
  override def close = {
    if (buf != null) {
      buf.release()
      buf = null
    }
  }
  override def adoptOpenResult: DataRef = {
    val b = buf
    buf = null
    DataRef.open(
      RefCounted(
        MemoryResource(buf.m, new Closeable {
          def close = {
            b.release()
          }
        }), 0))
  }
}

