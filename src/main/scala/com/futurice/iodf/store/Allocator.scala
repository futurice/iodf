package com.futurice.iodf.store

import java.io.{Closeable, OutputStream}

import com.futurice.iodf.Utils.using
import org.slf4j.LoggerFactory
import xerial.larray.buffer.{LBuffer, LBufferConfig, Memory}
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

trait Ref[T] extends Handle{
  def get : T
  def copy : Ref[T]
  def openMapped[E](f:T => E) : Ref[E] = {
    val c : Ref[T] = copy
    new Ref[E] {
      override def get: E = f(c.get)
      override def copy: Ref[E] = c.openMapped(f)
      override def close(): Unit = c.close
    }
  }
}

case class RefCount(closer:() => Unit, var v:Int = 0) {
  var isClosed = false
  def inc = synchronized { v += 1 }
  def dec = synchronized {
    v -= 1
    if (v == 0) {
      if (isClosed) throw new RuntimeException("double closing!")
      closer()
      isClosed = true
    }
  }
}

object Ref {
  def apply[T](value:T, refCount:RefCount) : Ref[T] = new Ref[T] {
    refCount.inc
    override def get: T = {
      if (refCount.isClosed) throw new RuntimeException("closed " + value + " accessed")
      value
    }
    override def copy: Ref[T] = Ref.apply[T](value, refCount)
    override def close(): Unit = refCount.dec
  }
  def apply[T](value:T, closer:() => Unit ): Ref[T] = {
    apply[T](value, new RefCount(closer, 0))
  }
  def apply[T <: Closeable](value:T): Ref[T] = {
    apply[T](value, () => value.close)
  }
}

trait DataCreator extends OutputStream {
  def pos : Long
  /**
   * this provides ability to access previously written data, while it is being written
   * the data reference is guaranteed to begin from the begin of created memory area,
   * and end exactly at the point, or somewhere after this data was being written.
   */
  def openDataRef : DataRef
  /* closes and finalizes the data, and returns an open data reference */
  def adoptResult : DataRef
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

trait AllocateOnce extends Closeable {
  def create : DataCreator
}

trait Allocator extends AllocateOnce {
  def create : DataCreator
/*  def openRef : DataCreatorRef = new DataCreatorRef {
    override def create = Allocator.this.create
    override def close = Unit
  }*/
}

object LBufferCreator {
  val defaultBufferSize = 1024
}

object DataRef {
  def open(memRef:Ref[Memory], from:Long = 0, sz:Option[Long] = None) : DataRef = {
    new DataRef {
      val ref = memRef.copy
      val f = from
      override def open: RandomAccess =
        using (new RandomAccess(ref)) { ref =>
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

class LBufferCreator extends DataCreator {
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
    ref = Some(Ref[LBuffer](m, () => m.release()))
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
  override def adoptResult: DataRef = {
    val rv = openDataRef
    close
    rv
  }
  override def openDataRef: DataRef = {
    using (ref.get.openMapped(_.m)) { ref =>
      DataRef.open(ref, 0, Some(pos))
    }
  }
}


class RamAllocator extends Allocator {
  override def create: DataCreator = {
    new LBufferCreator
  }
  override def close(): Unit = {}
}