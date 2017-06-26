package com.futurice.iodf.ioseq

import java.io.DataOutputStream

import com.futurice.iodf.store.{DataRef, Dir, IoData, RandomAccess}
import com.futurice.iodf._
import com.futurice.iodf.utils.{PeekIterable, PeekIterator, Scannable, Scanner}
import xerial.larray.LArray

import scala.reflect.runtime.universe._
import xerial.larray.buffer.LBufferAPI

/**
  * Created by arau on 24.11.2016.
  */
abstract class IoArray[Id, T](val ref:IoRef[Id, _ <: IoObject[Id]],
                              val buf:RandomAccess)
  extends IoSeq[Id, T] with PeekIterable[T] {
  def offset = 8L
  val lsize = buf.getBeLong(0)
  override def close(): Unit = {
    buf.close()
  }
  override def iterator =
    new PeekIterator[T] {
      var at = 0L
      def hasNext = at < lsize
      override def headOption =
        hasNext match {
          case true => Some(head)
          case false => None
        }
      override def head = IoArray.this.apply(at)
      def next = {
        val rv = head
        at += 1
        rv
      }
    }
}

abstract class IoArrayType[Id, T](implicit t:TypeTag[Seq[T]], vTag:TypeTag[T])
  extends IoTypeOf[Id, IoArray[Id, T], Seq[T]]()(t)
  with IoSeqType[Id, T, LSeq[T], IoArray[Id, T]] {
  def writeUnit(output:DataOutputStream, v:T) : Unit
  def newInstance(ref:IoRef[Id, IoArray[Id, T]], buf:RandomAccess) : IoArray[Id, T]
  def unitByteSize : Long
  def write(output:DataOutputStream, size:Long, data:Iterator[T]) = {
    output.writeLong(size)
    var written = 0
    data.foreach {  i =>
      writeUnit(output, i)
      written += 1
    }
    if (written != size) throw new IllegalArgumentException("size was " + size + " yet " + written + " entries was written.")
  }
  def write(output:DataOutputStream, data:Seq[T]) = {
    write(output, data.size, data.iterator)
  }
  def writeSeq(output:DataOutputStream, data:LSeq[T]) = {
    write(output, data.size, data.iterator)
  }
  override def viewMerged(items:Seq[LSeq[T]]) = {
    new MultiSeq[T, LSeq[T]](items.toArray)
  }
  def writeMerged2(output:DataOutputStream, items:Seq[(Long, Iterator[T])]) = {
    output.writeLong(items.map(_._1).sum)
    items.foreach { _._2.foreach { writeUnit(output, _ ) } }
  }
  override def writeMerged(output:DataOutputStream, items:Seq[LSeq[T]]) = {
    writeMerged2(output, items.map(e => (e.lsize, e.iterator)))
  }
  def open(data:IoData[Id]) = {
    newInstance(IoRef(this, data.ref), data.openRandomAccess)
  }
  def valueTypeTag = vTag
}

class IntIoArray[Id](ref:IoRef[Id, _ <: IoObject[Id]], buf:RandomAccess)
  extends IoArray[Id, Int](ref, buf) {
  override def apply(l: Long) : Int = {
    buf.getBeInt(offset + l*4)
  }
}

class IntIoArrayType[Id](implicit t:TypeTag[Seq[Int]], vTag:TypeTag[Int]) extends IoArrayType[Id, Int]()(t, vTag) {
  override def unitByteSize: Long = 4
  def writeUnit(out:DataOutputStream, v:Int) = out.writeInt(v)
  def newInstance(ref: IoRef[Id, IoArray[Id, Int]], buf: RandomAccess) =
    new IntIoArray(ref, buf)
}

class LongIoArray[Id](ref:IoRef[Id, _ <: IoObject[Id]], buf:RandomAccess)
  extends IoArray[Id, Long](ref, buf) {
  override def apply(l: Long) : Long= {
    buf.getBeLong(offset + l*8)
  }
}

class LongIoArrayType[Id](implicit t:TypeTag[Seq[Long]], vTag:TypeTag[Long]) extends IoArrayType[Id, Long]()(t, vTag) {
  override def unitByteSize: Long = 8
  def writeUnit(out:DataOutputStream, v:Long) = out.writeLong(v)
  def newInstance(ref: IoRef[Id, IoArray[Id, Long]], buf: RandomAccess) =
    new LongIoArray(ref, buf)
}
