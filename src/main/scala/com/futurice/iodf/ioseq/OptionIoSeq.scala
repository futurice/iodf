package com.futurice.iodf.ioseq

import com.futurice.iodf._
import com.futurice.iodf.io.{DataAccess, DataOutput, IoObject, IoRef}
import com.futurice.iodf.util._

import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe._

/**
  * Created by arau on 12.7.2017.
  */
class OptionIoSeq[T](ref:IoRef[OptionIoSeq[T]], indexes:LSeq[Long], values:LSeq[T], override val lsize:Long)
  extends IoSeq[Option[T]] with OptionLSeq[T] {
  override def openRef = ref.openCopy

  override def defined = LBits.from(indexes, lsize)
  override def definedStates = values
  override def definedStatesWithIndex = values zip indexes

  override def close = {
    ref.close
    indexes.close
    values.close
  }

  override def apply(l: Long): Option[T] = {
    val at = Utils.binarySearch(indexes, l)._1
    if (at != -1) {
      Some(values(at))
    } else {
      None
    }
  }

  /**
   * More efficient iterator implementation
   */
  override def iterator = new Iterator[Option[T]] {
    var at = 0
    var i = PeekIterator(indexes.iterator)
    var v = values.iterator
    override def hasNext: Boolean = at < lsize

    override def next(): Option[T] = {
      if (i.hasNext && at == i.head) {
        i.next
        val rv = v.next
        at += 1
        Some(rv)
      } else {
        at += 1
        None
      }
    }
  }
}

object OptionIoSeqType {

  def apply[T](tSeqType:SeqIoType[T, LSeq[T], _ <: LSeq[T]],
               longSeqType:SeqIoType[Long, LSeq[Long], _ <: LSeq[Long]])(
    implicit ifaceTag:TypeTag[LSeq[Option[T]]], instanceTag: TypeTag[OptionIoSeq[T]],valueTag:TypeTag[Option[T]]) =
    new OptionIoSeqType[T](tSeqType, longSeqType)
}

class OptionIoSeqType[T](valueSeqType:SeqIoType[T, LSeq[T], _ <: LSeq[T]],
                         longSeqType:SeqIoType[Long, LSeq[Long], _ <: LSeq[Long]])(
  implicit ifaceTag:TypeTag[LSeq[Option[T]]], instanceTag: TypeTag[OptionIoSeq[T]],valueTag:TypeTag[Option[T]])
  extends SeqIoType[Option[T], LSeq[Option[T]], OptionIoSeq[T]] {

  val interfaceType = ifaceTag.tpe
  val ioInstanceType = instanceTag.tpe
  val valueTypeTag = valueTag

  override def write(out: DataOutput, iface: LSeq[Option[T]]): Unit = {
    // FIXME: bad memory behavior and limited to 2G Some-values!
    val indexes = ArrayBuffer[Long]()
    iface.zipWithIndex.foreach { case (opt, index)  =>
      if (opt.isDefined) {
        indexes += index
      }
    }
    val begin = out.pos // should be 0
    out.writeLong(iface.lsize)
    longSeqType.write(out, LSeq.from(indexes))
    val valuesPos = out.pos
    valueSeqType.write(out, new LSeq[T] {
      override def apply(l: Long): T = iface(indexes(l.toInt)).get
      override def lsize: Long = indexes.size
    })
    out.writeLong(valuesPos - begin)
  }

  override def open(ref: DataAccess): OptionIoSeq[T] = scoped { implicit bind =>
    val size = ref.getBeLong(0)
    val seqPos = ref.getBeLong(ref.size - 8) // should be the last one in this buffer
    val index = longSeqType.open(ref.view(8, seqPos))
    val values = valueSeqType.open(ref.view(seqPos, ref.size-8))

    new OptionIoSeq[T](
      IoRef.open[OptionIoSeq[T]](
        this,
        ref.dataRef.openCopy),
      index,
      values,
      size)
  }

  override def openMerged(seqs: Seq[Ref[LSeq[Option[T]]]]): LSeq[Option[T]] = {
    new MultiSeq[Option[T], LSeq[Option[T]]](seqs.toArray)
  }

  override def defaultInstance(lsize: Long): Option[LSeq[Option[T]]] =
    Some(LSeq.fill(lsize, None))

}
