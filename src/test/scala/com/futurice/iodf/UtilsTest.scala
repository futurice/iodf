package com.futurice.iodf

import com.futurice.iodf.util.LSeq
import com.futurice.testtoys.TestSuite

import scala.util.Random

/**
  * Created by arau on 9.6.2017.
  */
class UtilsTest extends TestSuite("utils") {

  test("binarySearch") { t =>
    def tBinarySearch[T](intend:String, sortedSeq:LSeq[T], target:T, from:Long=0, until:Option[Long] = None)(implicit ord:Ordering[T]) = {
      val (index, low, high) = Utils.binarySearch(sortedSeq, target, from, until.getOrElse(sortedSeq.lsize))
      def show(i:Long) = i match {
        case -1 => "-"
        case v if v < sortedSeq.lsize => sortedSeq(v)
        case v => "eos"
      }
      t.tln(f"${intend}binarySearch(sortedSeq, $target, $from, $until)=${(index, low, high)} -> ${(show(index), show(low), show(high))}")
    }

    val rnd = new Random(1)
    val max = 128
    val seq = LSeq[Int]((0 until max).filter(i => rnd.nextInt(16) == 0))
    t.tln("seq is: " + seq.mkString(","))
    t.tln
    t.tln("boundaries:")
    tBinarySearch("  ",seq, 0)
    tBinarySearch("  ",seq, max)
    t.tln
    t.tln("outside boundaries:")
    tBinarySearch("  ",seq, -1)
    tBinarySearch("  ",seq, max+1)
    t.tln
    t.tln("random lookups:")
    (0 until 3).map(i =>
      tBinarySearch("  ", seq, rnd.nextInt(max)))
    t.tln
    t.tln("matches:")
    (0 until 3).map(i =>
      tBinarySearch("  ",seq, seq(rnd.nextInt(seq.size))))

    t.tln
    t.tln("bounded lookup area:")
    t.tln
    (0 until 4).map { i =>

      val from = rnd.nextInt(seq.size)
      val until = from + rnd.nextInt(seq.size - from)
      t.tln(f"  from $from to $until: ${(from.until(until)).map(i => seq(i)).mkString(", ")}")
      t.tln
      t.tln( "    random lookups:")
      (0 until 3).map(i =>
        tBinarySearch("      ",seq, from + rnd.nextInt(max), from, Some(until)))
      t.tln
      t.tln( "    matches:")
      (0 until 3).map(i =>
        tBinarySearch("      ", seq, seq(rnd.nextInt(seq.size)), from, Some(until)))
      t.tln
    }

  }

}
