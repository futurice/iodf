package com.futurice.iodf.perf

import java.util

import com.futurice.iodf.Utils._
import com.futurice.iodf.ioseq._
import com.futurice.iodf.store.{RamDir}
import com.futurice.iodf.util._
import com.futurice.iodf.{IoContext, IoScope}
import com.futurice.testtoys.{TestSuite, TestTool}

import scala.util.Random

/**
  * Put the perf tests here, which are too slow to run as sanity checks
  *
  * Created by arau on 7.6.2017.
  */
class BitsPerf extends TestSuite("perf/bits") {

  test("bits-perf") { t =>
    Tracing.lightTrace {
      scoped { implicit scope =>
        val sizes = Array(16, 256, 4 * 1024, 1024 * 1024)

        sizes.foreach { sz =>
          using(new RamDir[String]) { dir =>
            val bits = new SparseIoBitsType()
            val data = (0 until sz).filter(_ % 4 == 0).map(_.toLong)

            val n = 32
            t.t(f"creating $n sparse bits of size $sz...")
            val (ms, _) =
              TestTool.ms(
                (0 until n).foreach { i =>
                  bits.openCreated(dir.ref("bits" + i), LBits.from(data, sz)).close()
                })

            val old = t.peekDouble
            val now = ms / n.toDouble
            t.i(f"$now%.3f ms / op")
            old match {
              case Some(oldV) =>
                t.iln(f", was $oldV%.3f ms")
                if (Math.abs(Math.log(oldV / now)) > Math.log(2)) {
                  t.fail
                  t.tln("  performance has CHANGED!")
                } else {
                  t.iln("  ok.")
                }
              case None =>
                t.iln
                t.iln("  first run.")
            }
          }
        }
      }
    }
  }

  def testBitPerf(t:TestTool, size:Int) = {
    scoped { implicit bind =>
      val dir = bind(new RamDir[String])

      val sparse = new SparseIoBitsType()
      val dense = new DenseIoBitsType()

      // 1 billion
      val density = 64
      val sparsity = 16 * 1024
      val sparsity2 = size / 1024 // 1024 samples

      t.t("creating sparse bits of size " + size + "...")
      val s = {
        val bits = (0 until size).filter(_ % sparsity == 0).map(_.toLong)
        t.tUsLn(
          bind(
            sparse.openCreated(
              dir.ref("sparse"),
              LBits.from(bits, size))))
      }

      t.t("creating really sparse bits of size " + size + "...")
      val s2 = {
        val bits = (0 until size).filter(_ % sparsity2 == 0).map(_.toLong)
        t.tUsLn(
          bind(
            sparse.openCreated(
              dir.ref("sparse2"),
              LBits.from(bits, size))))
      }

      t.t("populating bitset of size " + size + "...")
      val b = t.tUsLn({
        val rv = new util.BitSet(size)
        (0 until size).filter(_ % density == 0).foreach(rv.set(_))
        rv
      })

      t.t("creating dense bits of size " + size + "...")
      val d =
        t.tUsLn(
          bind(
            dense.openCreated(
              dir.ref("dense"),
              LBits.from(b, size))))
      t.tln
      t.t("counting sparse f..")
      val sf = t.tUsLn(s.f)
      t.t("counting really sparse f..")
      val s2f = t.tUsLn(s2.f)
      t.t("counting dense f..")
      val df = t.tUsLn(d.f)
      t.t("counting bitset f..")
      val bf = t.tUsLn(b.cardinality())
      t.tln

      t.t("counting sparse & sparse..")
      val ss =
        t.tUsLn(
          s.fAnd(s))
      t.t("counting really sparse & really sparse..")
      val ss2 =
        t.tUsLn(
          s2.fAnd(s2))
      t.t("counting dense & dense..")
      val dd =
        t.tUsLn(
          d.fAnd(d))
      t.t("counting sparse & dense..")
      val sd =
        t.tUsLn(
          s.fAnd(d))
      t.t("counting really sparse & dense..")
      val sd2 =
        t.tUsLn(
          s2.fAnd(d))
      t.t("counting bitset & bitset..")
      val bb =
        t.tUsLn({
          val b2 = new util.BitSet(size)
          b2.or(b)
          b2.and(b)
          b2.cardinality()
        })
      t.tln
      t.tln(f"sparse: f $sf, dense f: $bf, bitset: $bf")
      t.tln(f"sparse&sparse: $ss, dense&dense:, $dd sparse&dense: $sd, bitset&bitset: $bb")
    }

  }

  test("bits-M-perf") { t =>
    testBitPerf(t, 1000000)
  }

  // 32 bits with each bit being of one megabyte?



  def makeBits[IoId](create:LBits => LBits,
                     p:Double,
                     n:Long,
                     rnd:Random) = {
    create(
      LBits.from((0L until n).filter(i => rnd.nextDouble() < p), n))
  }

  def perOpUs(op : => Unit, waitMs:Long = 100) = {
    val before = System.currentTimeMillis()
    var n = 0
    while (System.currentTimeMillis() - before < 100) {
      op
      n += 1
    }
    val after = System.currentTimeMillis()
    (1000*(after-before) / n.toDouble)
  }
  def tPerOpUs(t:TestTool, indent:String, name:String, op : => Unit, waitMs:Long = 100) = {
    t.t(f"$indent${name + " took "}%-16s ")
    val old = t.peekDouble
    val now = perOpUs(op, waitMs)
    t.i(f"$now%.3fs us")
    old match {
      case Some(o) =>
        t.i(f", was $o%.3f us ")
        if (o > now *2 ) {
          t.tln(" - better!")
        } else if (o * 2 < now ) {
          t.tln(" - WORSE!")
        } else {
          t.iln(" - ok ")
        }

      case None =>
        t.tln
    }
  }

  def tTestPerf[IoId](t:TestTool, a:LBits) = {
    using(IoScope.open) { implicit scope =>
      implicit val io = IoContext()
      tPerOpUs(t, "  ", "popcount", a f)
      tPerOpUs(t, "  ", "negation", a~)
    }
  }

  def tTestPerf[IoId](t:TestTool, a:LBits, b:LBits) = {
    using(IoScope.open) { implicit scope =>
      implicit val io = IoContext()
      tPerOpUs(t, "    ", "fAnd", a fAnd b)
      tPerOpUs(t, "    ", "&", a & b)
      tPerOpUs(t, "    ", "&~", a &~ b)
      tPerOpUs(t, "    ", "merge", a merge b)
    }
  }

  test("perf-by-sparsity") { t =>
    Tracing.lightTrace {
      using(IoScope.open) { implicit scope =>
        val rnd = new Random(0)
        implicit val io = IoContext()
        t.t("creating bits..")
        val n = 1024*1024
        val mbits =
          t.iMsLn(
            Array(0, 2, 4, 6, 8, 10, 12, 14).map(s => Math.pow(0.5, s)).map { p =>
              (p, makeBits(v => IoBits(v), p, n, rnd))
            })
        mbits.foreach { case (p1,b1) =>
          t.tln(f"p(a)=$p1%.5f, type:" + b1.getClass.getName)
          tTestPerf(t, b1)
          t.tln
          mbits.foreach { case (p2,b2) =>
            t.tln(f"  p(b)=$p2%.5f, type:" + b2.getClass.getName)
            tTestPerf(t, b1, b2)
            t.tln
          }
        }
      }
    }
  }

  test("multi-bits") { t =>
    Tracing.lightTrace {
      using (IoScope.open) { implicit bind =>
        val rnd = new Random(0)
        implicit val io = IoContext()
        t.t("creating multibits..")
        val n = 1024*1024
        val parts = 32
        val mbits =
          t.iMsLn(
            Array(4, 8, 12).map(s => Math.pow(0.5, s)).map { p =>
              (p, bind(MultiBits.donate(
                (0 until parts).map(i =>
                  Ref.open(makeBits(v => IoBits.create(v), p, n / parts, rnd))))
              ))
            })
        t.t("creating bits of different sparseness..")
        val bits =
          t.iMsLn(
            Array(4, 8, 12).map(s => Math.pow(0.5, s)).map { p =>
              (p, makeBits(v => IoBits(v), p, n, rnd))
            })

/*        t.t("sharding previous bits..")
        val sbits =
          bits.map { case (p, bits) =>
            (p, MultiBits.shard(bits, mbits.head._2))
          }*/

        val all = mbits ++ bits //++ sbits

        all.foreach { case (p1,b1) =>
          t.tln(f"p(a)=$p1%.5f, type:" + b1.getClass.getName)
          tTestPerf(t, b1)
          t.tln
          all.foreach { case (p2,b2) =>
            t.tln(f"  p(b)=$p2%.5f, type:" + b2.getClass.getName)
            tTestPerf(t, b1, b2)
            t.tln
          }
        }
      }
    }
  }
}
