package com.futurice.iodf.store

import java.io.{Closeable, OutputStream}

import com.futurice.iodf.Utils.using
import org.slf4j.LoggerFactory

import xerial.larray.buffer.{LBuffer, LBufferConfig}
import xerial.larray.mmap.{MMapBuffer, MMapMode}

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * Handles are used for lifecyles & resource management.
 * You can copy a handle, or you can close a handle
 */
trait Handle extends Closeable {
  def copy : Handle
}

trait DataCreator extends OutputStream {
  /* returned reference is open */
  def adoptResult : DataRef
}

trait DataCreatorRef {
  def create : DataCreator
}

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
  def open     : RandomAccess
  def byteSize : Long
  def copy     : DataRef

  def openView(from:Long, until:Long) : DataRef
}

class DataRefView(viewed:DataRef,
                  from:Long,
                  until:Long) extends DataRef {
  val v        = viewed.copy
  def close    = v.close
  def copy     = new DataRefView(v, from, until)
  def open     =
    using (v.open) { _.openView(from, until) }
  def byteSize = until - from
  def openView(from:Long, until:Long) = {
    new DataRefView(v, this.from + from, this.from + until)
  }
}

trait DataLand extends Closeable {
  def create : DataCreator
  def openRef : DataCreatorRef = new DataCreatorRef {
    override def create = DataLand.this.create
  }
}

object LBufferCreator {
  val defaultBufferSize = 1024
}

object DataRef {
  def open(memRef:RefCounted[MemoryResource], from:Long = 0, sz:Option[Long] = None) : DataRef = {
    new DataRef {
      memRef.inc
      val f = from
      override def open: RandomAccess =
        new RandomAccess(memRef)
      def copy = DataRef.open(memRef, from, sz)
      override def byteSize: Long =
        sz.getOrElse(memRef.value.memory.size())

      override def openView(from: Long, until: Long): DataRef =
        DataRef.open(memRef, f + from, Some(until))

      override def close(): Unit = memRef.close
    }
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
  override def adoptResult: DataRef = {
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

