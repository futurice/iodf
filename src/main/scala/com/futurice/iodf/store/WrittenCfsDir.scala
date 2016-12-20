package com.futurice.iodf.store

import java.io.{DataOutputStream, File, IOException, OutputStream}

import com.futurice.iodf.ioseq.Serializer

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Created by arau on 14.12.2016.
  */
class WrittenCfsDir[IoId : ClassTag, DirIoId](
   val ref:FileRef[DirIoId], idIo:Serializer[IoId], fromInt: Int => IoId) extends Dir[IoId] {

  val out = new DataOutputStream(ref.openOutput)

  val idPos = new ArrayBuffer[(IoId, Long)];

  var ready = true

  override def id(i: Int) = fromInt(i)

  //  def create(id:Id, length:Long) : IoData[Id]
  override def openOutput(id:IoId): OutputStream = {
    if (!ready) throw new IllegalStateException("cfd dir can be written one file at a time.")
    idPos += ((id, out.size()))
    ready = false
    new OutputStream {
      override def write(b:Int): Unit = {
        out.write(b)
      }
      override def write(b:Array[Byte]) {
        out.write(b)
      }
      override def write(b:Array[Byte], off:Int , len:Int) {
        out.write(b, off, len)
      }
      override def close = {
        ready = true
      }
    }
  }

  override def open(id:IoId, pos:Long): IoData[IoId] = {
    throw new IllegalStateException("WritteCfsDir can only be written, it cannot be opened")
  }
  override def list = idPos.map(_._1).toArray

  override def close(): Unit = {
    val indexPos = out.size
    out.writeInt(idPos.size)
    idPos.foreach { case (id, pos) =>
        idIo.write(out, id)
        out.writeLong(pos)
    }
    out.writeLong(indexPos)
    out.close()
  }
}

/**
  * Created by arau on 14.12.2016.
  */
class CfsDir[IoId : ClassTag, DirIoId](val ref:FileRef[DirIoId], idIo:Serializer[IoId], fromInt: Int => IoId) extends Dir[IoId] {

  val buf = ref.open
  val ra = buf.openRandomAccess

  val (dataSize, idPos) = {
    val indexPos = ra.getBeLong(ra.size-8)
    val indexSize = ra.getBeInt(indexPos)
    var at = indexPos + 4
    (indexPos,
     (0 until indexSize) map { i =>
       val id = idIo.read(ra, at)
       at += idIo.size(ra, at)
       val pos = ra.getBeLong(at)
       at += 8
       (id, pos)
     })
  }

  override def id(i: Int) = fromInt(i)
  override def openOutput(id:IoId): OutputStream = {
    throw new IllegalStateException("compound files are immutable once written")
  }
  override def open(id:IoId, pos:Long): IoData[IoId] = {
    idPos.indexWhere(_._1 == id) match {
      case -1 => throw new IllegalArgumentException(id + " not found")
      case i =>
        val begin = idPos(i)._2 + pos
        val end = (if (i + 1 == idPos.size) dataSize else idPos(i + 1)._2)
        val sz = end - begin
        new IoData[IoId] {
          override def close(): Unit = {}
          override def ref: DataRef[IoId] = DataRef(CfsDir.this, id, pos)
          override def openRandomAccess = ra.openSlice(begin, sz)
          override def size: Long = sz

          override def openView(offset: Long): IoData[IoId] =
            open(id, pos+offset)
        }
    }
  }
  override def list: Array[IoId] = idPos.map(_._1).toArray
  override def close(): Unit = {
    ra.close()
    buf.close()
  }
}
