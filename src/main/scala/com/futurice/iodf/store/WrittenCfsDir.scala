package com.futurice.iodf.store

import java.io._

import com.futurice.iodf._
import com.futurice.iodf.Utils._
import com.futurice.iodf.ioseq.{IoSeq, IoSeqType, Serializer}
import com.futurice.iodf.util.LSeq

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag


/**
  * Created by arau on 14.12.2016.
  */
class WrittenCfsDir[MyFileId](
   val ref:AllocateOnce,
   idSeqType:IoSeqType[MyFileId, _ >: LSeq[MyFileId], _ <: IoSeq[MyFileId]],
   longSeqType:IoSeqType[Long, _ >: LSeq[Long], _ <: IoSeq[Long]],
   fromInt: Int => MyFileId)(
   implicit tag:ClassTag[MyFileId],
   idOrdering:Ordering[MyFileId]) extends WritableDir[MyFileId] {

  val out = ref.create
/*  val out = new OutputStream {
    val o = new BufferedOutputStream(creator)
    var size = 0L

    override def write(b: Int): Unit = {
      o.write(b)
      size += 1
    }

    override def write(b: Array[Byte]) {
      o.write(b)
      size += b.size
    }

    override def write(b: Array[Byte], off: Int, len: Int) {
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
  }*/

  val ids = new ArrayBuffer[MyFileId];
  val pos = new ArrayBuffer[Long];

  var ready = true

  override def id(i: Int) = fromInt(i)

  //  def create(id:Id, length:Long) : IoData[Id]
  override def create(id: MyFileId) : DataCreator = {
    if (!ready) throw new IllegalStateException("cfd dir can be written one file at a time.")
    ids += id
    val begin = out.pos
    pos += begin
    ready = false
    new DataCreator {
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
        ready = true
      }
      override def openDataRef = {
        out.flush()
        using(out.openDataRef) { ref =>
          ref.openView(begin, out.pos)
        }
      }
      override def adoptResult: DataRef = {
        close
        openRef(id)
      }
    }
  }

  override def open(id: MyFileId): RandomAccess = {
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
      using (ref.openView(begin, end)) { _.open }
    }
  }

  override def list = ids.toArray

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
    using (new DataOutputStream(out)) { dout =>
      idSeqType.write(dout, LSeq(idIndex.map(_._1).toSeq))
      val ordPos = out.pos
      longSeqType.write(dout, LSeq(idIndex.map(_._2.toLong).toSeq))
      val posPos = out.pos
      longSeqType.write(dout, LSeq(pos))

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
class CfsDir[FileId](val ref:DataRef,
                     idSeqType:IoSeqType[FileId, _ <: LSeq[FileId], _ <: IoSeq[FileId]],
                     longSeqType:IoSeqType[Long, _ <: LSeq[Long], _ <: IoSeq[Long]],
                     fromInt: Int => FileId)(
                     implicit tag:ClassTag[FileId],
                     idOrdering:Ordering[FileId]) extends Dir[FileId] {

  val bind = IoScope.open
  val ra = bind(ref.open)

  val (dataSize, idSeq, ordSeq, posSeq) = {
    val idSeqPos = ra.getBeLong(ra.size-24)
    val ordSeqPos = ra.getBeLong(ra.size-16)
    val posSeqPos = ra.getBeLong(ra.size-8)
    val rv =
       (idSeqPos,
        bind(idSeqType.open  (bind(ref.openView(idSeqPos,  ordSeqPos)))),
        bind(longSeqType.open(bind(ref.openView(ordSeqPos, posSeqPos)))),
        bind(longSeqType.open(bind(ref.openView(posSeqPos, ra.size)))))
    rv
  }

  override def id(i: Int) = fromInt(i)
  override def byteSize(id: FileId) = {
    Utils.binarySearch(idSeq, id)(idOrdering)._1 match {
      case -1 => throw new IllegalArgumentException(id + " not found")
      case i =>
        val ord = ordSeq(i)
        val begin = posSeq(ord)
        val end = (if (ord + 1 == idSeq.lsize) dataSize else posSeq(ord + 1))
        end - begin
    }
  }

  override def open(id:FileId): RandomAccess = {
    Utils.binarySearch(idSeq, id)(idOrdering)._1 match {
      case -1 => throw new IllegalArgumentException(id + " not found")
      case i =>
        val ord = ordSeq(i)
        val begin = posSeq(ord)
        val end = (if (ord + 1 == idSeq.lsize) dataSize else posSeq(ord + 1))
        Utils.using (ref.openView(begin, end)) { _.open }
    }
  }
  override def list: Array[FileId] = idSeq.toArray
  override def close(): Unit = {
    bind.close
  }
  def byteSize = ra.size
}
