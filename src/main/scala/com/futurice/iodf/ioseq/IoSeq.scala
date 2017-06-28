package com.futurice.iodf.ioseq

import java.io.DataOutputStream

import com.futurice.iodf.util.LSeq
import com.futurice.iodf.IoType
import com.futurice.iodf.io.{IoObject, IoType}

trait IoIterable[T] extends IoObject with Iterable[T] {
  def iterator : Iterator[T]
}

trait IoSeqType[Member, Interface <: LSeq[Member], Distributed <: IoSeq[Member] with Interface] extends IoType[Distributed] with WithValueTypeTag[Member] {
  def defaultSeq(lsize:Long) : Option[Interface] = None

  def viewMerged(seqs:Seq[Interface]) : Interface
  def viewAnyMerged(seqs:Seq[Any]) : Interface =
    viewMerged(seqs.map(_.asInstanceOf[Interface]))

  def writeSeq(out:DataOutputStream, v:Interface) : Unit
  def writeAnySeq(out:DataOutputStream, v:LSeq[_]) = {
    v match {
      case i:Interface => writeSeq(out, i)
    }
  }
  def writeMerged(out:DataOutputStream, ss:Seq[Interface]) = {
    writeSeq(out, viewMerged(ss))
  }
  def writeAnyMerged(out:DataOutputStream, ss:Seq[Any]) = {
    writeMerged(out, ss.map(_.asInstanceOf[Interface]))
  }
}

trait IoSeq[T] extends IoIterable[T] with LSeq[T] {
}
