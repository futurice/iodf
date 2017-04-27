package com.futurice.iodf.ioseq

import java.io.DataOutputStream

import com.futurice.iodf.store.{Dir, IoData}
import com.futurice.iodf._
import com.futurice.iodf.Utils._
import xerial.larray.buffer.LBufferAPI

import scala.reflect.runtime.universe._

/**
  * Created by arau on 25.11.2016.
  */
class RefIoSeq[Id, T <: IoObject[Id]](
    override val ref:IoRef[Id, RefIoSeq[Id, T]],
    val types:IoTypes[Id],
    val buf:ObjectIoSeq[Id, (Int, Id, Long)]) extends IoSeq[Id, T] {
  def dir = ref.dataRef.dir // assume that the referred items are in the same directory
  override def apply(l: Long): T = {
    val (typ, id, pos) = buf(l)
    using (dir.open(id, pos)) { mem =>
      types.idIoType(typ).open(mem).asInstanceOf[T]
    }
  }
  override def lsize: Long = buf.size
  override def close(): Unit = buf.close
}

class RefIoSeqType[Id, T <: IoObject[Id]](
   types:IoTypes[Id],
   entryType:ObjectIoSeqType[Id, (Int, Id, Long)])(
   implicit val t : TypeTag[Seq[IoObject[Id]]],
   implicit val vTag : TypeTag[IoObject[Id]])
  extends IoTypeOf[Id, RefIoSeq[Id, T], Seq[IoObject[Id]]]()(t)
  with    SeqIoType[Id, RefIoSeq[Id, T], IoObject[Id]]{

  override def write(output: DataOutputStream, data: Seq[IoObject[Id]]) = {
    entryType.write(
      output,
      data.map { e =>
        (types.ioTypeId(e.ref.typ), e.ref.dataRef.id, e.ref.dataRef.pos) })
  }
  override def writeMerged(out: DataOutputStream, seqA: RefIoSeq[Id, T], seqB: RefIoSeq[Id, T]): Unit = {
    entryType.writeMerged(out, seqA.buf, seqB.buf)
  }

  override def open(buf: IoData[Id]): RefIoSeq[Id, T] = {
    new RefIoSeq[Id, T](
      IoRef[Id, RefIoSeq[Id, T]](this, buf.ref),
      types,
      entryType.open(buf))
  }
  override def valueTypeTag = vTag

}

/*    new RefIoSeq[Id, T](
      IoRef(this, dir, id),
      idsType.create(id, data.map { _.ref.posRef.id }, dir))*(
  }
  override def open(id: Id, dir: Dir[Id], pos: Long) = {
    new RefIoSeq[Id, T](
      IoRef(this, dir, id, pos),
      idsType.open(id, dir, pos))
  }
}
*/
/*class IoRefIo[Id, T](
      w:OutputWriting[Id],
      r:LBufferReading[Id]) extends OutputWriting[IoRef[Id, T]] with LBufferReading[IoRef[Id, T]] {
  override def write(o: DataOutputStream, v: IoRef[Id, T]): Unit = {

  }

  override def read(o: LBufferAPI, pos: Long): IoRef[Id, T] = {
    val size = o.getInt(pos)
    val bytes = new Array[Byte](size)
    o.copyTo(pos.toInt+4, bytes, 0, size)
    new String(bytes)
  }
}
*/