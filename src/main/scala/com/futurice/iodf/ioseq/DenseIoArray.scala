package com.futurice.iodf.ioseq

import java.io.DataOutputStream

import com.futurice.iodf.store._
import com.futurice.iodf._
import com.futurice.iodf.io.{IoObject, IoRef}
import com.futurice.iodf.util._

import scala.reflect.runtime.universe._

/**
  * Created by arau on 24.11.2016.
  */
abstract class IoArray[T](ref:IoRef[IoArray[T]],
                          val buf:RandomAccess)
  extends IoSeq[T] with PeekIterable[T] {
  def offset = 8L
  val lsize = buf.getBeLong(0)
  override def close(): Unit = {
    ref.close
    buf.close()
  }
  override def openRef = ref.copy
  override def iterator : PeekIterator[T] =
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

abstract class IoArrayType[T]()(implicit ifaceTag:TypeTag[LSeq[T]], instanceTag: TypeTag[IoArray[T]],valueTag:TypeTag[T])
  extends IoSeqType[T, LSeq[T], IoArray[T]] {

  val interfaceType = ifaceTag.tpe
  val ioInstanceType = instanceTag.tpe
  val valueTypeTag = valueTag

  def writeUnit(output:DataOutputStream, v:T)              : Unit
  def newInstance(ref:IoRef[IoArray[T]], buf:RandomAccess) : IoArray[T]
  def unitByteSize                                         : Long

  def write(output:DataOutputStream, size:Long, data:Iterator[T]) = {
    output.writeLong(size)
    var written = 0
    data.foreach {  i =>
      writeUnit(output, i)
      written += 1
    }
    if (written != size) throw new IllegalArgumentException("size was " + size + " yet " + written + " entries was written.")
  }
  def write(output:DataOutputStream, data:LSeq[T]) = {
    write(output, data.size, data.iterator)
  }
  override def viewMerged(items:Seq[LSeq[T]]) = {
    new MultiSeq[T, LSeq[T]](items.toArray)
  }
  def open(data:DataRef) = {
    newInstance(IoRef(this, data), data.open)
  }
}

class IntIoArray(ref:IoRef[IoArray[Int]], buf:RandomAccess)
  extends IoArray[Int](ref, buf) {
  override def apply(l: Long) : Int = {
    buf.getBeInt(offset + l*4)
  }
}

class IntIoArrayType(implicit ifaceTag:TypeTag[LSeq[Int]], instanceTag: TypeTag[IoArray[Int]], valueTag:TypeTag[Int])
  extends IoArrayType[Int]()(ifaceTag, instanceTag, valueTag) {
  override def unitByteSize: Long = 4
  def writeUnit(out:DataOutputStream, v:Int) = out.writeInt(v)
  def newInstance(ref: IoRef[IoArray[Int]], buf: RandomAccess) =
    new IntIoArray(ref, buf)
}

class LongIoArray(ref:IoRef[IoArray[Long]], buf:RandomAccess)
  extends IoArray[Long](ref, buf) {
  override def apply(l: Long) : Long= {
    buf.getBeLong(offset + l*8)
  }
}

class LongIoArrayType(implicit ifaceTag:TypeTag[LSeq[Long]], instanceTag: TypeTag[IoArray[Long]], valueTag:TypeTag[Long])
  extends IoArrayType[Long]()(ifaceTag, instanceTag, valueTag) {
  override def unitByteSize: Long = 8
  def writeUnit(out:DataOutputStream, v:Long) = out.writeLong(v)
  def newInstance(ref: IoRef[IoArray[Long]], buf: RandomAccess) =
    new LongIoArray(ref, buf)
}
