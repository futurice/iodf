package com.futurice.iodf.store

import java.io._

import com.futurice.iodf.{IoScope, IoSeq, SeqIoType, Utils}
import com.futurice.iodf.ioseq.Serializer

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/**
  * Created by arau on 14.12.2016.
  */
class WrittenCfsDir[IoId, DirIoId](
   val ref:FileRef[DirIoId],
   idSeqType:SeqIoType[DirIoId, _ <: IoSeq[DirIoId, IoId], IoId],
   longSeqType:SeqIoType[DirIoId, _ <: IoSeq[DirIoId, Long], Long],
   fromInt: Int => IoId)(
   implicit tag:ClassTag[IoId],
   idOrdering:Ordering[IoId]) extends Dir[IoId] {

  val out = new OutputStream {
    val o = new BufferedOutputStream(ref.openOutput)
    var size = 0L
    override def write(b:Int): Unit = {
      o.write(b)
      size += 1
    }
    override def write(b:Array[Byte]) {
      o.write(b)
      size += b.size
    }
    override def write(b:Array[Byte], off:Int , len:Int) {
      o.write(b, off, len)
      size += len
    }
    override def flush() {
      o.flush()
    }
    @throws[IOException]
    override def close(): Unit = {
      o.close()
    }
  }

  val ids  = new ArrayBuffer[IoId];
  val pos = new ArrayBuffer[Long];

  var ready = true

  override def id(i: Int) = fromInt(i)

  //  def create(id:Id, length:Long) : IoData[Id]
  override def openOutput(id:IoId): OutputStream = {
    if (!ready) throw new IllegalStateException("cfd dir can be written one file at a time.")
    ids += id
    pos += out.size
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
  override def open(id:IoId, pos:Long, size:Option[Long]): IoData[IoId] = {
    throw new IllegalStateException("WritteCfsDir can only be written, it cannot be opened")
  }
  override def list = ids.toArray

  override def close(): Unit = {
    val idIndex = ids.zipWithIndex.toArray.sortBy(_._1)
    val idPos = out.size
    val dout = new DataOutputStream(out)
    idSeqType.writeAny(dout, idIndex.map(_._1).toSeq)
    val ordPos = out.size
    longSeqType.writeAny(dout, idIndex.map(_._2.toLong).toSeq)
    val posPos = out.size
    longSeqType.writeAny(dout, pos)

    /*    out.writeInt(idPos.size)
     idPos.foreach { case (id, pos) =>
        idIo.write(out, id)
        out.writeLong(pos)
    }*/
    dout.writeLong(idPos)
    dout.writeLong(ordPos)
    dout.writeLong(posPos)
    dout.close()
  }
}

/**
  * Created by arau on 14.12.2016.
  */
class CfsDir[IoId, DirIoId](val ref:FileRef[DirIoId],
                            idSeqType:SeqIoType[DirIoId, _ <: IoSeq[DirIoId, IoId], IoId],
                            longSeqType:SeqIoType[DirIoId, _ <: IoSeq[DirIoId, Long], Long],
                            fromInt: Int => IoId)(
                            implicit tag:ClassTag[IoId],
                            idOrdering:Ordering[IoId]) extends Dir[IoId] {

  val bind = IoScope.open
  val buf = bind(ref.open)
  val ra = bind(buf.openRandomAccess)

  val (dataSize, idSeq, ordSeq, posSeq) = {
    val idSeqPos = ra.getBeLong(ra.size-24)
    val ordSeqPos = ra.getBeLong(ra.size-16)
    val posSeqPos = ra.getBeLong(ra.size-8)
    val rv =
       (idSeqPos,
        bind(idSeqType.open(bind(buf.openView(idSeqPos, Some(ordSeqPos - idSeqPos))))),
        bind(longSeqType.open(bind(buf.openView(ordSeqPos, Some(posSeqPos - ordSeqPos))))),
        bind(longSeqType.open(bind(buf.openView(posSeqPos, Some(ra.size - posSeqPos))))))
    rv
  }

/*  val (dataSize, idPos) = {
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
  }*/

  override def id(i: Int) = fromInt(i)
  override def openOutput(id:IoId): OutputStream = {
    throw new IllegalStateException("compound files are immutable once written")
  }
  override def open(id:IoId, pos:Long, size:Option[Long]): IoData[IoId] = {
    Utils.binarySearch(idSeq, id)(idOrdering) match {
      case -1 => throw new IllegalArgumentException(id + " not found")
      case i =>
        val ord = ordSeq(i)
        val begin = posSeq(ord) + pos
        val end = (if (ord + 1 == idSeq.lsize) dataSize else posSeq(ord + 1))
        val sz = size.getOrElse(end - begin)
        new IoData[IoId] {
          override def close(): Unit = {}
          override def ref: DataRef[IoId] = DataRef(CfsDir.this, id, pos)
          override def openRandomAccess = ra.openSlice(begin, sz)
          override def size: Long = sz

          override def openView(offset: Long, size:Option[Long]): IoData[IoId] =
            open(id, pos+offset, size)
        }
    }
  }
  override def list: Array[IoId] = idSeq.toArray
  override def close(): Unit = {
    bind.close
  }
}
