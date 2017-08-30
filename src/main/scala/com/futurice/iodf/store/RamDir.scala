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
        dir.put(id, creator.openDataRef).foreach { _.close()  }
        creator.close
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
  override def openAccess(id: FileId): DataAccess = synchronized {
    dir(id).openAccess
  }
  override def list = synchronized {
    LSeq.from(dir.keySet.toArray.sorted)
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
