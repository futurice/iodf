package com.futurice.iodf.ioseq

import java.io.DataOutputStream

import com.futurice.iodf.util.LSeq
import com.futurice.iodf.io.{IoObject, IoType, WithValueTypeTag}

trait IoIterable[T] extends IoObject with Iterable[T] {
  def iterator : Iterator[T]
}

trait IoSeqType[Member, Interface <: LSeq[Member], IoInstance <: IoSeq[Member] with Interface]
  extends IoType[Interface, IoInstance] with WithValueTypeTag[Member] {

  def defaultSeq(lsize:Long) : Option[Interface] = None // is this even needed?
  def viewMerged(seqs:Seq[Interface]) : Interface

  // helper functions, do we need these?
  def writeMerged(out:DataOutputStream, ss:Seq[Interface]) = {
    write(out, viewMerged(ss))
  }
  def viewAnyMerged(seqs:Seq[Any]) : Interface =
    viewMerged(seqs.map(_.asInstanceOf[Interface]))
  def writeAnySeq(out:DataOutputStream, v:LSeq[_]) = {
    v match {
      case i:Interface => write(out, i)
    }
  }
  def writeAnyMerged(out:DataOutputStream, ss:Seq[Any]) = {
    writeMerged(out, ss.map(_.asInstanceOf[Interface]))
  }
}

trait IoSeq[T] extends IoIterable[T] with LSeq[T] {}
