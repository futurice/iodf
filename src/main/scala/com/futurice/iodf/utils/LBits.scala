package com.futurice.iodf.utils

import java.util

import com.futurice.iodf.ioseq.{DenseIoBits, IoBits}
import com.futurice.iodf._


/**
  * Created by arau on 31.5.2017.
  */
trait LBits extends LSeq[Boolean] {

  def isDense = LBits.isDense(f, n)
  def isSparse = LBits.isSparse(f, n)

  def trues : Scannable[Long, Long]
  def f : Long
  def n = lsize
  def view(from:Long, until:Long) : LBits = new BitsView(this, from, until)

  def fAnd(bits:LBits) = {
    LBits.fAnd(this, bits)
  }
  def leLongs : Iterable[Long] = new Iterable[Long] {
    val longs = longCount
    def iterator =
      new Iterator[Long] {
        val ts = trues.iterator
        var i = 0L
        override def hasNext = i < longs
        override def next = {
          var rv = 0L
          while (ts.hasNext && ts.head < (i+1)*64) {
            rv |= (1L << (ts.next()-i*64))
          }
          i += 1
          rv
        }
      }
  }

  def longCount = DenseIoBits.bitsToLongCount(n)

  // NOTE, these should be moved to same separate trait, where they can be imported from

  def createAnd[IoId2](b:LBits)(implicit io:IoContext[IoId2]) : LBits = {
    io.bits.createAnd(io.dir, this, b)
  }
  def createAndNot[IoId2](b:LBits)(implicit io:IoContext[IoId2]) : LBits = {
    io.bits.createAndNot(io.dir, this, b)
  }
  def createNot[IoId2](implicit io:IoContext[IoId2]) = {
    io.bits.createNot(io.dir, this)
  }
  def createMerged[IoId2](b:LBits)(implicit io:IoContext[IoId2]) : LBits = {
    io.bits.createMerged(io.dir, Seq(this, b))
  }
  def &[IoId2](b:LBits)(implicit io:IoContext[IoId2], scope:IoScope) : LBits = {
    scope.bind(createAnd(b))
  }
  def &~[IoId2](b:LBits)(implicit io:IoContext[IoId2], scope:IoScope) : LBits = {
    scope.bind(createAndNot(b))
  }
  def ~[IoId2](implicit io:IoContext[IoId2], scope:IoScope) : LBits = {
    scope.bind(createNot)
  }
  def merge [IoId2](b:LBits)(implicit io:IoContext[IoId2], scope:IoScope) : LBits = {
    scope.bind(createMerged(b))
  }


}

class BitsView(bits:LBits, from:Long, until:Long) extends LBits {
  private lazy val truesBegin = {
    val rv = bits.trues.iterator
    rv.seek(from)
    rv
  }
  lazy val f = trues.size.toLong

  def lsize = (until-from)
  def apply(v:Long) = trues.iterator.seek(v)

  private def truesView(i:Scanner[Long, Long]) : Scanner[Long, Long] = {
    new Scanner[Long, Long] {
      def hasNext = i.hasNext && i.head < until
      def next = (i.next-from)
      def copy = truesView(i.copy)

      override def headOption: Option[Long] = i.headOption.map(_-from)
      override def head = i.head-from
      override def seek(t: Long): Boolean = i.seek(t+from)
    }
  }

  def trues =
    new Scannable[Long, Long] {
      def iterator = truesView(truesBegin.copy)
    }
}

object LBits {
  def denseSparseSplit = 256L

  def isDense(f: Long, n: Long) = {
    f * denseSparseSplit > n
  }
  def isDense(bits: LBits): Boolean = isDense(bits.f, bits.n)
  def isSparse(f: Long, n: Long): Boolean = !isDense(f, n)
  def isSparse(bits: LBits): Boolean = isSparse(bits.f, bits.n)

  def fAnd(a: LBits, b: LBits): Long = {
    (a.isSparse, b.isSparse) match {
      case (false, false) => fAndDenseDense(a, b)
      case (false, true) => fAndSparseDense(b, a)
      case (true, false) => fAndSparseDense(a, b)
      case (true, true) => fAndSparseSparse(a, b)
    }
  }

  def fAndDenseDense(a: LBits, b: LBits): Long = {
    val at = a.leLongs.iterator
    val bt = b.leLongs.iterator
    var rv = 0
    while (at.hasNext) {
      rv += java.lang.Long.bitCount(at.next() & bt.next())
    }
    rv
  }

  def fAndSparseDense(sparse: LBits, dense: LBits): Long = {
    var rv = 0L
    // assume dense to be random accessible
    for (t <- sparse.trues) {
      if (dense(t)) rv += 1L
    }
    rv
  }

  def fAndSparseSparse(a: LBits, b: LBits): Long = {
    var rv = 0L
    val i1 = PeekIterator(a.trues.iterator)
    val i2 = PeekIterator(b.trues.iterator)
    while (i1.hasNext && i2.hasNext) {
      val t1 = i1.head
      val t2 = i2.head
      if (t1 < t2) i1.next
      else if (t1 > t2) i2.next
      else {
        rv += 1
        i1.next
        i2.next
      }
    }
    rv
  }
  def apply(bools:Seq[Boolean]) = {
    new LBits {
      lazy val f = bools.count(b => b).toLong
      def lsize = bools.length
      def apply(i:Long) = bools(i.toInt)
      def truesFrom(from:Long) : Scanner[Long, Long] = {
        new Scanner[Long, Long] {
          var at = from.toInt
          def nextTrue: Unit = {
            while (at < bools.length && !bools(at)) at += 1
          }
          nextTrue
          override def copy: Scanner[Long, Long] = truesFrom(at)
          override def head: Long = at
          override def seek(t: Long): Boolean = {
            at = t.toInt
            nextTrue
            at == t
          }
          override def hasNext: Boolean = at < bools.length
          override def next(): Long = {
            val rv = head
            at += 1
            nextTrue
            rv
          }
        }
      }
      override def trues = new Scannable[Long, Long] {
        def iterator = truesFrom(0)
      }
    }
  }
  def apply(bits:util.BitSet, n:Long) = {
    val thisn = n
    new LBits {
      lazy val f = bits.cardinality().toLong
      val lsize = thisn
      def apply(i:Long) = {
        bits.get(i.toInt)
      }
      override def iterator = new Iterator[Boolean] {
        var i = 0;
        def hasNext = i < lsize
        def next = {
          val rv = bits.get(i)
          i += 1
          rv
        }
      }
      def truesFromBit(fromBit:Int) : Scanner[Long, Long] = {
        new Scanner[Long, Long] {
          var n = bits.nextSetBit(fromBit)
          def hasNext = n >= 0 && n < lsize
          def next = {
            val rv = n
            n = bits.nextSetBit(n+1)
            rv
          }
          def copy = truesFromBit(n)
          override def head: Long = n
          override def seek(t: Long): Boolean = {
            n = bits.nextSetBit(t.toInt)
            n == t
          }
        }
      }
      override def trues = new Scannable[Long, Long]{
        override def iterator = truesFromBit(0)
      }
    }
  }

  def empty(n:Long) = {
    val thisn = n
    new LBits {
      override def lsize = thisn
      override def f: Long = 0
      override def fAnd(bits: LBits): Long = 0
      override def trues =
        new Scannable[Long, Long] {
          def iterator = new Scanner[Long, Long] {
            override def head = throw new IllegalStateException()
            override def copy = this
            override def seek(t: Long): Boolean = false
            override def hasNext: Boolean = false
            override def next(): Long = -1
          }
        }

      override def leLongs = new Iterable[Long] {
        def iterator = new Iterator[Long] {
          var l = 0
          val max = DenseIoBits.bitsToLongCount(lsize)

          def hasNext = l < max

          def next = {
            l += 1
            0L
          }
        }
      }
      override def apply(l: Long): Boolean = false
    }
  }

  def apply(trueIndexes:Seq[Long], n:Long) : LBits =
    apply(LSeq(trueIndexes), n)

  def apply(trueIndexes:LSeq[Long], n:Long) : LBits = {
    def thisn = n

    new LBits {
      def f = trueIndexes.size

      def apply(l: Long) = {
        Utils.binarySearch(trueIndexes, l)._1 != -1
      }

      private def truesFromTrue(fromTrue:Long) : Scanner[Long, Long] = {
        new Scanner[Long, Long] {
          var at = fromTrue

          def copy = truesFromTrue(at)
          def head = trueIndexes(at)

          def hasNext = at < trueIndexes.size

          def next = {
            val rv = head
            at += 1
            rv
          }

          def seek(target: Long) = {
            val (hit, low, high) =
              Utils.binarySearch(trueIndexes, target, at, at + (target - head) + 1)
            at = high
            hit != -1
          }
        }
      }

      override def trues = new Scannable[Long, Long] {
        override def iterator = truesFromTrue(0)
      }

      override def iterator = new Iterator[Boolean] {
        var i = 0;
        var t = PeekIterator(trueIndexes.iterator)

        def hasNext = i < lsize

        def next = {
          if (t.hasNext && t.head == i) {
            t.next
            i += 1
            true
          } else {
            i += 1
            false
          }
        }
      }
      override val lsize: Long = thisn
    }
  }

}
