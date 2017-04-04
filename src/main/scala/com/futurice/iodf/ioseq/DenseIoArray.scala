package com.futurice.iodf.ioseq

import java.io.DataOutputStream

import com.futurice.iodf.store.{DataRef, Dir, IoData, RandomAccess}
import com.futurice.iodf._
import xerial.larray.LArray

import scala.reflect.runtime.universe._
import xerial.larray.buffer.LBufferAPI

/**
  * Created by arau on 24.11.2016.
  */
abstract class IoArray[Id, T](val ref:IoRef[Id, _ <: IoObject[Id]],
                              val buf:RandomAccess)
  extends IoSeq[Id, T] {
  def offset = 8L
  val lsize = buf.getBeLong(0)
  override def close(): Unit = {
    buf.close()
  }
}

abstract class IoArrayType[Id, T](implicit t:TypeTag[Seq[T]], vTag:TypeTag[T])
  extends IoTypeOf[Id, IoArray[Id, T], Seq[T]]()(t)
  with SeqIoType[Id, IoArray[Id, T], T] {
  def writeUnit(output:DataOutputStream, v:T) : Unit
  def newInstance(ref:IoRef[Id, IoArray[Id, T]], buf:RandomAccess) : IoArray[Id, T]
  def unitByteSize : Long
  def write(output:DataOutputStream, size:Long, data:Iterator[T]) = {
    output.writeLong(size)
    data.foreach { writeUnit(output, _) }
  }
  def write(output:DataOutputStream, data:Seq[T]) = {
    write(output, data.size, data.iterator)
  }
  def writeMerged2(output:DataOutputStream, a:IoArray[Id, T], bSize:Long, b:Iterator[T]) = {
    output.writeLong(a.lsize + bSize)
    a.buf.writeTo(a.offset, output, unitByteSize*a.lsize)
    b.foreach { writeUnit(output, _) }
  }
  def writeMerged(output:DataOutputStream, a:IoArray[Id, T], b:IoArray[Id, T]) = {
    output.writeLong(a.lsize + b.lsize)
    a.buf.writeTo(a.offset, output, unitByteSize*a.lsize)
    b.buf.writeTo(b.offset, output, unitByteSize*b.lsize)
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
