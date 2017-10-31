package com.futurice.iodf.ioseq


import com.futurice.iodf._
import com.futurice.iodf.util.{LSeq, Ref}
import com.futurice.iodf.io._

trait IoIterable[T] extends IoObject with Iterable[T] {
  def iterator : Iterator[T]
}


trait SeqIoType[Member, Interface <: LSeq[Member], IoInstance <: IoSeq[Member] with Interface]
  extends MergeableIoType[Interface, IoInstance] with WithValueTypeTag[Member] with SizedMerging[Interface] {

  def defaultInstance(lsize:Long) : Option[Interface] = None // is this even needed?

  def writeAnySeq(out:DataOutput, v:LSeq[_]) = {
    v match {
      case i:Interface => write(out, i)
    }
  }
  def viewAnyMerged(seqs:Seq[Ref[Any]]) : Interface =
    openViewMerged(seqs.map(_.map(_.asInstanceOf[Interface])))
  def writeAnyMerged(out:DataOutput, ss:Seq[Ref[Any]]) = {
    writeMerged(out, ss.map(_.map(_.asInstanceOf[Interface])))
  }
}

trait IoSeq[T] extends IoIterable[T] with LSeq[T] {

  def seqIoType : SeqIoType[T, _ <: LSeq[T], _ <: IoSeq[T]] =
    ioType.asInstanceOf[SeqIoType[T, _ <: LSeq[T], _ <: IoSeq[T]]]

}
