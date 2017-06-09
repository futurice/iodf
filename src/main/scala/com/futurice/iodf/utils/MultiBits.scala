package com.futurice.iodf.utils

import com.futurice.iodf.{IoContext, IoScope, MultiSeq}
import com.futurice.iodf.ioseq.IoBits

object MultiBits {
  //  def apply[IoId](bits:Array[IoBits[_]]) = new MultiBits(bits)
  def apply(bits:Seq[LBits]) = new MultiBits(bits.toArray)

  /* it's faster to do bit operations, if both bits have the same scheme */
  def shard[IoId](sharded:LBits, model:MultiBits)(implicit scope:IoScope, io:IoContext[IoId]): MultiBits = {
    scope.bind(new MultiBits(
      model.ranges.map { case (from, until) =>
        io.bits.create(sharded.view(from, until))
      }))
  }
}

class MultiBits(val bits:Array[LBits]) extends MultiSeq[Boolean, LBits](bits) with LBits {
  override def f: Long = {
    bits.map(_.f).sum
  }

  override def apply(l: Long): Boolean = {
    val Some((s, sIndex)) = toSeqIndex(l)
    s(sIndex)
  }

  def mapOperation[T, E](bs: LBits,
                         map:(LBits, LBits)=>T,
                         reduce:Seq[T]=>E,
                         fallback:(LBits, LBits)=>E) = {
    bs match {
      case b : MultiBits =>
        reduce((bits zip b.bits).map { case (a, b) => map(a, b) })
      case _ =>
        fallback(this, bs)
    }
  }


  override def fAnd(bs: LBits): Long = {
    mapOperation(bs, _ fAnd _, (vs : Seq[Long]) => vs.sum, LBits.fAnd(_, _))
  }
  private def truesWith(iters:Seq[Scanner[Long, Long]]) : Scanner[Long, Long] = new Scanner[Long, Long]{
    val is = PeekIterator((iters zip ranges).iterator)
    def prepareNext: Unit = {
      while (is.hasNext && !is.head._1.hasNext) is.next
    }
    prepareNext

    def copy = truesWith(iters.map(_.copy))

    override def hasNext: Boolean = {
      is.hasNext && is.head._1.hasNext
    }

    override def next : Long = {
      val (it, (begin, end)) = is.head
      val rv = it.next + begin
      prepareNext
      rv
    }

    override def headOption: Option[Long] = {
      is.headOption.flatMap { case (it, (begin, _)) => it.headOption.map(_ + begin) }
    }
    override def head : Long = {
      val (it, (begin, end)) = is.head
      it.head + begin
    }

    override def seek(t: Long): Boolean = {
      is.headOption.map { case (it, (begin, end)) =>
        it.seek(t - begin) match {
          case true => true
          case _ if it.hasNext => false
          case _ =>
            prepareNext
            seek(t) // seek in the next segment
        }
      }.getOrElse(false)
    }
  }
  override def trues = new Scannable[Long, Long] {
    override def iterator = truesWith(bits.map(_.trues.iterator))
  }

  def createAnd[IoId1, IoId2](b:IoBits[IoId1])(implicit io:IoContext[IoId2]) = {
    mapOperation(
      b, _ createAnd _, MultiBits.apply _,
      (a, b) => io.bits.createAnd(io.dir.ref(io.dir.freeId), a, b))
  }
  def createAndNot[IoId1, IoId2](b:IoBits[IoId1])(implicit io:IoContext[IoId2]) = {
    mapOperation(b, _ createAndNot _, MultiBits.apply _,
      (a, b) => io.bits.createAndNot(io.dir.ref(io.dir.freeId), a, b))
  }
  def createNot[IoId1, IoId2](implicit io:IoContext[IoId2]) = {
    MultiBits(bits.map(_.createNot))
  }
  def createMerged[IoId1, IoId2](b:IoBits[IoId1])(implicit io:IoContext[IoId2]) = {
    io.bits.createMerged(io.dir, Seq(this, b))
  }

}

