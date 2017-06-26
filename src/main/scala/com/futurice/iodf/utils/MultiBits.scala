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
  def maybeShard[IoId](sharded:LBits, model:LBits)(implicit scope:IoScope, io:IoContext[IoId]): LBits
  = {
    model match {
      case b:MultiBits  => shard(sharded, b)
      case _ =>            sharded
    }
  }
}

class MultiBits(val bits:Array[LBits]) extends MultiSeq[Boolean, LBits](bits) with LBits {
  override lazy val f: Long = {
    bits.map(_.f).sum
  }

  override def leLongs : Iterable[Long] = new Iterable[Long] {
    def iterator = new Iterator[Long] {
      val is = PeekIterator((bits.map(_.leLongs.iterator)  zip ranges).iterator)

      var overbits = 0L
      var overflow = 0L
      var readFromCurrent = 0L

      def getNextLeLongFrom(v: Long) = {
        if (overbits > 0) {
          val rv = (v << overbits) | overflow
          overflow = (v >>> (64 - overbits))
          rv
        } else {
          v
        }
      }

      def getNextLeLong : Option[Long] = {
        if (is.hasNext && is.head._1.hasNext) {
          val (i, (begin, end)) = is.head
          val l = i.next
          readFromCurrent += 64
          if (i.hasNext) {
            Some(getNextLeLongFrom(l))
          } else { // handle the last long in a special way
            var extra = (end-begin) - (readFromCurrent-64)
            val left = overbits + extra
            if (left >= 64) { // there is one word full of content
              val rv = getNextLeLongFrom(l)
              overbits = (left - 64).toInt
              Some(rv)
            } else { // pad the extra bits to overflow
              val ov = overflow
              overflow = l << overbits
              overflow = overflow | ov
              overbits = left.toInt
              getNextLeLong
            }
          }
        } else if (is.hasNext) {
          is.next
          readFromCurrent = 0L
          getNextLeLong
        } else if (overbits > 0) {
          val rv = overflow
          overbits = 0
          Some(rv)
        } else {
          None
        }
      }
      var n = getNextLeLong
      def hasNext = n.isDefined
      def next = {
        val rv = n
        n = getNextLeLong
        rv.get
      }
    }
  }


  override def apply(l: Long): Boolean = {
    toSeqIndex(l) match {
      case Some((s, sIndex)) =>
        s(sIndex)
      case None =>
        throw new IllegalArgumentException(
          f"no page found for index $l (this length is $lsize, pages are ${ranges.mkString(",")})")
    }
  }

  def mapOperation[T, E](bs: LBits,
                         map:(LBits, LBits)=>T,
                         reduce:Seq[T]=>E,
                         fallback:(LBits, LBits)=>E) = {
    bs match {
      case b : MultiBits =>
        reduce((bits zip b.bits).map { case (a, b) => map(a, b) })
      case _ =>
//        reduce((bits zip ranges).map { case (b, (begin, end)) => map(b, bs.view(begin, end)) })
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
      is.headOption.flatMap { case (it, (begin, _)) =>
        it.headOption.map(_ + begin)
      }
    }
    override def head : Long = {
      val (it, (begin, end)) = is.head
      it.head + begin
    }

    override def seek(t: Long): Boolean = {
      is.headOption.map { case (it, (begin, end)) =>
        if (t < begin) {
          false
        } else if (t >= end) {
          it.seek(end) // finish this one
          prepareNext
          seek(t)
        } else {
          it.seek(t - begin) match {
            case true => true
            case _ if it.hasNext => false
            case _ =>
              prepareNext
              seek(t) // seek in the next segment
          }
        }
      }.getOrElse(false)

    }
  }
  override def trues = new Scannable[Long, Long] {
    override def iterator = truesWith(bits.map(_.trues.iterator))
  }

  override def createAnd[IoId2](b:LBits)(implicit io:IoContext[IoId2]) = {
    mapOperation(
      b, _ createAnd _, MultiBits.apply _,
      (a, b) => io.bits.createAnd(io.dir.ref(io.dir.freeId), a, b))
  }
  override def createAndNot[IoId2](b:LBits)(implicit io:IoContext[IoId2]) = {
    mapOperation(b, _ createAndNot _, MultiBits.apply _,
      (a, b) => io.bits.createAndNot(io.dir.ref(io.dir.freeId), a, b))
  }
  override def createNot[IoId2](implicit io:IoContext[IoId2]) = {
    MultiBits(bits.map(_.createNot))
  }
  override def createMerged[IoId2](b:LBits)(implicit io:IoContext[IoId2]) = {
    io.bits.createMerged(io.dir, Seq(this, b))
  }

}

