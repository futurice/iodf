package com.futurice.iodf.store

import com.futurice.iodf.io.{DataAccess, DataOutput, DataOutputMixin, DataRef}
import com.futurice.iodf.util.LSeq
import com.futurice.iodf._
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.reflect.ClassTag

class RamDir[FileId:Ordering](implicit t:ClassTag[FileId]) extends MutableDir[FileId] {

  val logger = LoggerFactory.getLogger(getClass)

  val defaultBufferSize = 1024*4

  val allocator = new RamAllocator

  val dir = mutable.HashMap[FileId, DataRef]()

  var topId : Option[FileId] = None

  def lsize = dir.size

  override def create(id:FileId) = {
    val self = this
    val creator = allocator.create
    new DataOutput with DataOutputMixin{
      override def close = self.synchronized {
        dir.put(id, creator.dataRef)
        creator.close
      }
      override def dataRef: DataRef =
        creator.dataRef
      override def write(b: Int): Unit =
        creator.write(b)
      override def write(b: Array[Byte], off: Int, len: Int): Unit = {
        creator.write(b, off, len)
      }

      override def pos: Long = creator.pos
    }
  }

  override def delete(id:FileId) = RamDir.this.synchronized {
    dir.remove(id).isDefined
  }
  override def rename(from:FileId, to:FileId) = RamDir.this.synchronized {
    dir.remove(from).map { e =>
      dir.put(to, e)
      true
    }.getOrElse(false)
  }
  override def access(id: FileId): DataAccess = synchronized {
    dir(id).access
  }
  override def list = synchronized {
    LSeq.from(dir.keySet.toArray.sorted)
  }
  def byteSize(id:FileId) : Long = dir(id).byteSize
  def byteSize : Long = dir.map(_._2.byteSize).sum
  override def close(): Unit = synchronized {
    dir.clear()
  }

}
