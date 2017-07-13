package com.futurice.iodf.store

import com.futurice.iodf._
import com.futurice.iodf.io.{DataOutput, _}
import com.futurice.iodf.ioseq.{IoSeq, SeqIoType, Serializer}
import com.futurice.iodf.util.LSeq

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._


object WrittenCfsDir {
  def open[CfsFileId:TypeTag:ClassTag:Ordering](out:DataOutput)(implicit io:IoContext) = {
    val fileSeqType  = io.types.seqTypeOf[CfsFileId]
    val longSeqType = io.types.longLSeqType
    new WrittenCfsDir[CfsFileId](out, fileSeqType, longSeqType)
  }

}
/**
  * Created by arau on 14.12.2016.
  */
class WrittenCfsDir[MyFileId](_out:DataOutput,
                              idSeqType:SeqIoType[MyFileId, _ >: LSeq[MyFileId], _ <: IoSeq[MyFileId]],
                              longSeqType:SeqIoType[Long, _ >: LSeq[Long], _ <: IoSeq[Long]])(
   implicit tag:ClassTag[MyFileId],
   idOrdering:Ordering[MyFileId]) extends WritableDir[MyFileId] {

  val out = _out.subOutput

  val ids = new ArrayBuffer[MyFileId];
  val pos = new ArrayBuffer[Long];

  var ready = true

  //  def create(id:Id, length:Long) : IoData[Id]
  override def create(id: MyFileId) : io.DataOutput = {
    if (!ready) throw new IllegalStateException("cfd dir can be written one file at a time.")
    ids += id
    val begin = out.pos
    pos += begin
    ready = false
    new DataOutput with DataOutputMixin {
      override def pos = {
        out.pos - begin
      }
      override def write(b: Int): Unit = {
        out.write(b)
      }
      override def write(b: Array[Byte]) {
        out.write(b)
      }
      override def write(b: Array[Byte], off: Int, len: Int) {
        out.write(b, off, len)
      }
      override def close = {
        out.flush
        ready = true
      }
      override def openDataRef = {
        out.flush()
        using(out.openDataRef) { ref =>
          ref.openView(begin, out.pos)
        }
      }
    }
  }

  override def openAccess(id: MyFileId): DataAccess = {
    out.flush
    val i = ids.indexOf(id)
    if (i == -1) throw new IllegalArgumentException(id + " not found.")

    val end = (i, ready) match {
      case (i, true) if (i + 1 == this.pos.size) =>
        out.pos
      case (i, false) if (i + 1 == this.pos.size) =>
        throw new IllegalArgumentException(id + " not fully written yet.")
      case _ => this.pos(i + 1)
    }

    val p = this.pos(i)
    val begin = p
    using(out.openDataRef) { ref  =>
      using (ref.openView(begin, end)) { _.openAccess }
    }
  }

  override def list = LSeq.from(ids)

  override def byteSize(id: MyFileId) = {
    val i = ids.indexOf(id)
    val end =
      if (i + 1 == pos.size) {
        out.pos
      } else {
        pos(i+1)
      }
    (end - pos(i))
  }

  override def close(): Unit = {
    val idIndex = ids.zipWithIndex.toArray.sortBy(_._1)
    val idPos = out.pos
    using (out) { dout =>
      idSeqType.write(dout, LSeq.from(idIndex.map(_._1).toSeq))
      val ordPos = out.pos
      longSeqType.write(dout, LSeq.from(idIndex.map(_._2.toLong).toSeq))
      val posPos = out.pos
      longSeqType.write(dout, LSeq.from(pos))

      dout.writeLong(idPos)
      dout.writeLong(ordPos)
      dout.writeLong(posPos)
    } // dout.close()
  }

  def byteSize = out.pos

}

/**
  * Created by arau on 14.12.2016.
  */
class CfsDir[FileId](_data:DataAccess,
                     idSeqType:SeqIoType[FileId, _ <: LSeq[FileId], _ <: IoSeq[FileId]],
                     longSeqType:SeqIoType[Long, _ <: LSeq[Long], _ <: IoSeq[Long]])(
                     implicit tag:ClassTag[FileId],
                     idOrdering:Ordering[FileId]) extends IndexReferableDir[FileId] {

  val bind = IoScope.open
  val data = bind(_data.openCopy)
  override def close(): Unit = {
    bind.close
  }

  val (dataSize, idSeq, ordSeq, posSeq) = {
    val idSeqPos = data.getBeLong(data.size-24)
    val ordSeqPos = data.getBeLong(data.size-16)
    val posSeqPos = data.getBeLong(data.size-8)
    val rv =
       (idSeqPos,
        bind(idSeqType.open  (bind(data.openView(idSeqPos,  ordSeqPos)))),
        bind(longSeqType.open(bind(data.openView(ordSeqPos, posSeqPos)))),
        bind(longSeqType.open(bind(data.openView(posSeqPos, data.size)))))
    rv
  }

  override def indexRef(i:Long)  = {
    new DataRef {
      override def openAccess: DataAccess = openIndex(i)
      override def byteSize: Long = indexByteSize(i)
      override def openCopy: DataRef = this
      override def openView(from: Long, until: Long): DataRef = new DataRefView(this, from, until)
      override def close(): Unit = Unit
    }
  }

  override def indexByteSize(i:Long) = {
    val ord = ordSeq(i)
    val begin = posSeq(ord)
    val end = (if (ord + 1 == idSeq.lsize) dataSize else posSeq(ord + 1))
    end - begin
  }
  override def byteSize(id: FileId) = {
    Utils.binarySearch(idSeq, id)(idOrdering)._1 match {
      case -1 => throw new IllegalArgumentException(id + " not found")
      case i =>
        indexByteSize(i)
    }
  }
  override def openIndex(i:Long): DataAccess = {
    val ord = ordSeq(i)
    val begin = posSeq(ord)
    val end = (if (ord + 1 == idSeq.lsize) dataSize else posSeq(ord + 1))
    data.openView(begin, end)
  }
  override def openAccess(id:FileId): DataAccess = {
    Utils.binarySearch(idSeq, id)(idOrdering)._1 match {
      case -1 => throw new IllegalArgumentException(id + " not found")
      case i =>
        openIndex(i)
    }
  }
  override def list = idSeq
  def byteSize = data.size
}

object CfsDir {
  def open[CfsFileId:TypeTag:ClassTag:Ordering](data:DataAccess)(implicit io:IoContext) = {
    val fileSeqType  = io.types.seqTypeOf[CfsFileId]
    val longSeqType = io.types.longLSeqType
    new CfsDir[CfsFileId](data, fileSeqType, longSeqType)
  }

}