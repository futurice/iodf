package com.futurice.iodf.store
import java.io._

import org.slf4j.LoggerFactory
import xerial.larray.buffer.{LBuffer, LBufferConfig}
import xerial.larray.mmap.{MMapBuffer, MMapMode}

import scala.collection.mutable
import scala.reflect.ClassTag

trait IdSchema[Id] {
  def id(i:Int) : Id
  def max(a:Id, b:Id) : Id
  def nextId(a:Id) : Id
}

object IntIdSchema extends IdSchema[Int] {
  override def id(i: Int): Int = i

  override def max(a: Int, b: Int): Int = Math.max(a, b)

  override def nextId(a: Int): Int = a + 1
}
object StringIdSchema extends IdSchema[String] {
  override def id(i: Int): String = "_" + i

  override def max(a: String, b: String): String =
    if (a >= b) a else b

  override def nextId(a: String): String =
    a match {
      case id if id.startsWith("_") => "_" + (id.substring(1).toInt + 1)
    }
}

class RamDir[FileId](ids:IdSchema[FileId])(implicit t:ClassTag[FileId]) extends MutableDir[FileId] {

  val logger = LoggerFactory.getLogger(getClass)

  val defaultBufferSize = 1024*4

  val allocator = new RamAllocator

  val dir = mutable.HashMap[FileId, DataRef]()

  var topId : Option[FileId] = None

  override def freeId : FileId = { topId.map(ids.nextId(_)).getOrElse(ids.id(0)) }

  override def id(i: Int): FileId = ids.id(i)

  override def create(id:FileId) = {
    val creator = allocator.create
    new DataCreator {

      override def close = {
        adoptResult.close
      }
      override def adoptResult: DataRef = RamDir.this.synchronized {
        val res = creator.adoptResult
        dir.put(id, res.copy)
        res
      }
      override def openDataRef: DataRef =
        creator.openDataRef
      override def write(b: Int): Unit =
        creator.write(b)
      override def write(b: Array[Byte], off: Int, len: Int): Unit = {
        creator.write(b, off, len)
      }

      override def pos: Long = creator.pos
    }
  }

  override def delete(id:FileId) = RamDir.this.synchronized {
    dir.remove(id).map { e => e.close; true }.getOrElse(false)
  }

/*  override def openOutput(id: Id): OutputStream = {
    synchronized {
      topId = Some(topId.map(t => ids.max(id, t)).getOrElse(id))
    }
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
      override def close = synchronized {
        dir +=
          id ->
            IoData.open(
              FileDataRef[Id](RamDir.this, id, 0),
              RefCounted(
                MemoryResource(buf.m, new Closeable {
                  def close = {
  //                  logger.info("closing ram file of " +(pos/1024) + " KB")
                    buf.release()
                  }
                }),
                0))
//        logger.info((pos/1024) + "KB allocated, total -> " + (totalMemory / 1024) + "KB")
      }
    }
  }*/

  override def open(id: FileId): RandomAccess = synchronized {
    dir(id).open
  }
  override def list: Array[FileId] = synchronized {
    dir.keySet.toArray
  }
  def byteSize(id:FileId) : Long = dir(id).byteSize
  def byteSize : Long = dir.map(_._2.byteSize).sum
  override def close(): Unit = synchronized {
//    logger.info("closing ram dir of " + (totalMemory / 1024) + "KB")

    dir.foreach {
      _._2.close()
    }
    dir.clear()
  }

}

object RamDir {
  def apply() = new RamDir[Int](IntIdSchema)
}