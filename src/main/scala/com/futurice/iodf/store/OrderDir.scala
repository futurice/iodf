package com.futurice.iodf.store

import com.futurice.iodf.io._
import com.futurice.iodf.ioseq.{IoSeq, SeqIoType}
import com.futurice.iodf.store.OrderDir.open
import com.futurice.iodf.{IoContext, IoScope, Utils, io, using}
import com.futurice.iodf.util.LSeq

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

/*
 * Order directory is a directory, where you write the directory entries in order,
 * and you refer the directory entries by order.
 *
 * The benefits of the order directories are:
 *
 *   * limited size
 *   * O(1) lookup
 *
 * The const of the order directory is that:
 *
 *   * Only Int-references are allowed
 *   * you cannot write entries out of order
 */


class WrittenOrderDir(_out:DataOutput,
                      longSeqType:SeqIoType[Long, _ >: LSeq[Long], _ <: IoSeq[Long]]) extends WritableDir[Int] {

  val out = _out.subOutput
  val pos = new ArrayBuffer[Long];

  var ready = true

  def lsize = pos.size

  def lastRef(implicit bind:IoScope) = ref(pos.size)

  override def create(id: Int) : io.DataOutput = {
    if (!ready) throw new IllegalStateException("order dir can be written one file at a time.")
    if (id != pos.size) throw new IllegalStateException("order dir entries must be written in order")
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
      override def dataRef = {
        out.flush()
        using(out.dataRef) { ref =>
          ref.openView(begin, out.pos)
        }
      }
    }
  }

  override def access(i: Int): DataAccess = {
    out.flush
    val end = (i, ready) match {
      case (i, true) if (i + 1 == this.pos.size) =>
        out.pos
      case (i, false) if (i + 1 == this.pos.size) =>
        throw new IllegalArgumentException(i + " not fully written yet.")
      case _ => this.pos(i + 1)
    }

    val p = this.pos(i)
    val begin = p
    using(out.dataRef) { ref  =>
      using (ref.openView(begin, end)) { _.openAccess }
    }
  }

  override def list = LSeq.from((0 until pos.size))

  override def byteSize(i: Int) = {
    val end =
      if (i + 1 == pos.size) {
        out.pos
      } else {
        pos(i+1)
      }
    (end - pos(i))
  }

  override def close(): Unit = {
    using (out) { dout =>
      val posSeqPos = out.pos
      longSeqType.write(dout, LSeq.from(pos))
      dout.writeLong(posSeqPos)
    } // dout.close()
  }
  def byteSize = out.pos
}

class OrderDir(_data:DataAccess,
               longSeqType:SeqIoType[Long, _ <: LSeq[Long], _ <: IoSeq[Long]]) extends IndexReferableDir[Int] {
  val bind = IoScope.open
  val data = bind(_data.openCopy)
  override def close(): Unit = {
    bind.close
  }

  val (dataSize, posSeq) = {
    val posSeqPos = data.getBeLong(data.size-8)
    val rv =
      (posSeqPos,
       bind(longSeqType.apply(bind(data.view(posSeqPos, data.size)))))
    rv
  }

  def lsize = posSeq.lsize

  override def indexRef(i:Long)  = {
    new DataRef {
      override def openAccess: DataAccess = accessIndex(i)
      override def byteSize: Long = indexByteSize(i)
      override def openCopy: DataRef = this
      override def view(from: Long, until: Long): DataRef = new DataRefView(this, from, until)
      override def close(): Unit = Unit
    }
  }

  def refs(implicit bind:IoScope) : LSeq[DataRef] = list.map(ref(_))

  override def indexByteSize(ord:Long) = {
    val begin = posSeq(ord)
    val end = (if (ord + 1 == posSeq.lsize) dataSize else posSeq(ord + 1))
    end - begin
  }
  override def byteSize(ord: Int) = {
    indexByteSize(ord)
  }
  override def accessIndex(ord:Long): DataAccess = {
    val begin = posSeq(ord)
    val end = (if (ord + 1 == posSeq.lsize) dataSize else posSeq(ord + 1))
    data.view(begin, end)
  }
  override def access(ord:Int): DataAccess = {
    accessIndex(ord)
  }
  override def list = LSeq.from(0 until posSeq.size)
  def byteSize = data.size
}

object WrittenOrderDir {
  def open(out:DataOutput)(implicit io:IoContext) = {
    val longSeqType = io.types.longLSeqType
    new WrittenOrderDir(out, longSeqType)
  }
  def apply(out:DataOutput)(implicit bind:IoScope, io:IoContext) = {
    bind(open(out))
  }
}

object OrderDir {

  def open(data:DataAccess)(implicit io:IoContext) = {
    val longSeqType = io.types.longLSeqType
    new OrderDir(data, longSeqType)
  }
  def apply(data:DataAccess)(implicit bind:IoScope, io:IoContext) = {
    bind(open(data))
  }
}