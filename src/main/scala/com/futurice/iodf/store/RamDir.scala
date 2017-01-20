package com.futurice.iodf.store
import java.io._

import xerial.larray.buffer.{LBuffer, LBufferConfig}
import xerial.larray.mmap.{MMapBuffer, MMapMode}

import scala.collection.mutable
import scala.reflect.ClassTag

trait IdSchema[Id] {
  def id(i:Int) : Id
}

object IntIdSchema extends IdSchema[Int] {
  override def id(i: Int): Int = i
}
object StringIdSchema extends IdSchema[String] {
  override def id(i: Int): String = "_" + i
}

class RamDir[Id](ids:IdSchema[Id])(implicit t:ClassTag[Id]) extends Dir[Id] {

  val defaultBufferSize = 1024*4

  val allocator = LBufferConfig.allocator

  val dir = mutable.HashMap[Id, IoData[Id]]()

  override def id(i: Int): Id = ids.id(i)

  override def openOutput(id: Id): OutputStream = {
    new OutputStream {
      var buf = new LBuffer(defaultBufferSize)
      var pos = 0L
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
        dir +=
          id ->
            IoData.open(
              DataRef[Id](RamDir.this, id, 0),
              RefCounted(
                MemoryResource(buf.m, new Closeable {
                  def close = {
                    buf.release()
                  }
                })))
      }
    }
  }

  override def open(id: Id, pos: Long): IoData[Id] = {
    dir(id).openView(pos)
  }
  override def list: Array[Id] = {
    dir.keySet.toArray
  }
  override def close(): Unit = {
    dir.foreach {
      _._2.close()
    }
    dir.clear()
  }

}

object RamDir {
  def apply() = new RamDir[Int](IntIdSchema)
}