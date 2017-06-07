package com.futurice.iodf

import java.io.{DataOutputStream, File}
import java.util

import com.futurice.testtoys.{TestSuite, TestTool}
import com.futurice.iodf.store.{MMapDir, RamDir, RefCounted}
import com.futurice.iodf.Utils._
import com.futurice.iodf.ioseq._
import com.futurice.iodf.utils.{Bits, DenseBits, SparseBits}

import scala.reflect.runtime.universe._
import scala.util.Random
import scala.concurrent.duration._
import scala.concurrent.Await

/**
  * Created by arau on 7.6.2017.
  */
class BitsTest extends TestSuite("bits") {

  test("sparse-bits") { t =>
    RefCounted.trace {
      using(new MMapDir(t.fileDir)) { dir =>
        val bits = new SparseIoBitsType[String]()
        using(bits.create(dir.ref("bits"), new SparseBits(Seq(0L, 2L), 4))) { b =>

          t.tln("bit(3):        " + b(2));

          t.tln("bit count is   " + b.f + "/" + b.n)
          t.tln("true bits are: " + b.trues.mkString(", ") + " (" + b.trues.lsize + ")")
          t.tln("bits by index: " + (0L until b.n).map {
            b(_)
          }.mkString(", "))
          t.tln("bits by iter:  " + b.map { e: Boolean =>
            e.toString
          }.mkString(", "))
        }
      }
    }
  }

  test("dense-bits") { t =>
    RefCounted.trace {
      using(IoScope.open) { implicit bind =>
        val dir = bind(new MMapDir(t.fileDir))
        val bits = new BooleanIoSeqType[String]()

        val b1 = bind(bits.create(dir.ref("bits"), Seq(false, false, true, false, true)))
        val b2 = bind(bits.create(dir.ref("bits2"), Seq(false, true, true, false)))
        val b3 = bind(bits.create(dir.ref("bits3"), (0 until 150).map(_ % 3 == 0)))
        using(new DataOutputStream(dir.ref("bitsM2").openOutput)) { o =>
          bits.writeAnyMerged(o, b3, b1)
        }
        val bM2 = bind(bits.open(bind(dir.open("bitsM2"))))

        using(new DataOutputStream(dir.ref("bitsM").openOutput)) { o =>
          bits.writeAnyMerged(o, b1, b2)
        }
        val bM = bind(bits.open(bind(dir.open("bitsM"))))

        using(new DataOutputStream(dir.ref("bitsM3").openOutput)) { o =>
          bits.writeAnyMerged(o, b1, b3)
        }
        val bM3 = bind(bits.open(bind(dir.open("bitsM3"))))

        using(new DataOutputStream(dir.ref("bitsM4").openOutput)) { o =>
          bits.writeAnyMerged(o, b3, b3)
        }
        val bM4 = bind(bits.open(bind(dir.open("bitsM4"))))

        def tBits(b: DenseIoBits[String]) {
          t.tln("bit(2):        " + b(2));

          t.tln("bit count is     " + b.f + "/" + b.n)
          t.tln("true bits length " + b.trues.size)
          t.tln("true bits are:   " + b.trues.mkString(", "))
          t.tln("real true bits:  " + b.zipWithIndex.filter(_._1).map(_._2).mkString(", "))
          t.tln("bits by index:   " + (0L until b.n).map {
            b(_)
          }.mkString(", "))
          t.tln("bits by iter:    " + b.map { b: Boolean =>
            b.toString
          }.mkString(", "))
          if (b.f != b.trues.size) throw new RuntimeException
          t.tln
        }

        def findErrors[Id, T](col: IoSeq[Id, T], colA: IoSeq[Id, T], colB: IoSeq[Id, T]) = {
          (if (col.lsize != colA.lsize + colB.lsize)
            Seq(("SIZE", col.lsize, colA.lsize, colB.lsize))
          else Seq()) ++
            (0L until colA.lsize).filter { i =>
              col(i) != colA(i)
            }.map(i => ("A", i, col(i), colA(i))) ++
            (0L until colB.lsize).filter { i =>
              col(colA.lsize + i) != colB(i)
            }.map(i => ("B", colA.lsize + i, col(colA.lsize + i), colB(i)))
        }

        t.tln("b1")
        tBits(b1)
        t.tln("b2")
        tBits(b2)
        t.tln("b3")
        tBits(b3)
        t.tln("bM")

        tBits(bM)
        t.tln("errors: " + findErrors(bM, b1, b2).mkString(","))
        t.tln("bM2")

        tBits(bM2)
        t.tln("errors: " + findErrors(bM2, b3, b1).mkString(","))

        t.tln("bM3")
        tBits(bM3)
        t.tln("errors: " + findErrors(bM3, b1, b3).mkString(","))

        t.tln("bM4")
        tBits(bM4)
        t.tln("errors: " + findErrors(bM4, b3, b3).mkString(","))
      }
    }
  }

  test("bits") { t =>
    RefCounted.trace {
      scoped { implicit scope =>
        val dir = MMapDir(t.fileDir)
        val bits =
          new IoBitsType[String](
            new SparseIoBitsType[String](),
            new DenseIoBitsType[String]())
        t.tln("creating short bit vector:")
        val dense = bits(dir.ref("bits"), new SparseBits(Seq(0L, 2L), 4))

        t.tln("  bit type:      " + dense.unwrap.getClass.getName)

        t.tln("  bit(2):        " + dense(2));

        t.tln("  bit count is   " + dense.f + "/" + dense.n)
        t.tln("  true bits are: " + dense.trues.mkString(", ") + " (" + dense.trues.size + ")")
        t.tln("  bits by index: " + (0L until dense.n).map {
          dense(_)
        }.mkString(", "))
        t.tln("  bits by iter:  " + dense.map { e: Boolean =>
          e.toString
        }.mkString(", "))
        t.tln
        t.tln("creating dense vector:")
        val dense2 = bits(dir.ref("bits2"), new SparseBits((0L until 1024L).filter(i => i % 3 == 1), 1024))

        t.tln("  bit type:      " + dense2.unwrap.getClass.getName)

        t.tln("  bit(2):        " + dense2(2));

        t.tln("  bit count is   " + dense2.f + "/" + dense2.n)
        t.tln("  first trues:   " + dense2.trues.take(10).mkString(", ") + " (" + dense2.trues.size + ")")
        t.tln
        t.tln("creating very sparse bit vector:")

        val sparse =
          scope(
            bits.create(dir.ref("bits3"), new SparseBits(Seq(2L, 445L), 1024)))

        t.tln("  bit type:      " + sparse.unwrap.getClass)

        t.tln("  bit(2):        " + sparse(2));

        t.tln("  bit count is   " + sparse.f + "/" + sparse.n)
        t.tln("  true bits are: " + sparse.trues.mkString(", ") + " (" + sparse.trues.size + ")")
        t.tln

        implicit val io = IoContext()

        def tBits(b: IoBits[_]) = {
          t.tln("      bit type:      " + b.getClass)
          b match {
            case w: WrappedIoBits[String] =>
              t.tln("      internal type: " + w.unwrap.getClass)
            case _ =>
          }

          t.tln("      bit(2):        " + b(2));

          t.tln("      bit count is   " + b.f + "/" + b.n)
          t.tln("      first trues:   " + b.trues.take(10).mkString(", ") + " (" + b.trues.size + ")")
        }

        def tTest(bA: WrappedIoBits[String], bB: WrappedIoBits[String]) = {
          t.tln("    merge:")
          val bM = (bA merge bB).asInstanceOf[WrappedIoBits[String]]
          tBits(bM)
          if (bA.size == bB.size) {
            t.tln("    and:")
            val bAnd = (bA & bB)
            tBits(bAnd)
          }
        }

        val comb = Seq("short dense" -> dense,
          "long dense" -> dense2,
          "sparse" -> sparse,
          "empty" -> bits.defaultSeq(1024))

        for ((n1, b1) <- comb; (n2, b2) <- comb) {
          t.tln(f"  for $n1 and $n2:")
          tTest(b1, b2)
          t.tln
        }
      }
    }
  }


  test("bits-perf") { t =>
    RefCounted.trace {
      val sizes = Array(16, 256, 4 * 1024, 1024 * 1024)

      sizes.foreach { sz =>
        using(new MMapDir(t.fileDir)) { dir =>
          val bits = new SparseIoBitsType[String]()
          val data = (0 until sz).filter(_ % 4 == 0).map(_.toLong)

          val n = 32
          t.t(f"creating $n sparse bits of size $sz...")
          val (ms, res) =
            TestTool.ms(
              (0 until n).foreach { i =>
                bits.create(dir.ref("bits"), new SparseBits(data, sz)).close()
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

  def testBitPerf(t:TestTool, size:Int) = {
    using(IoScope.open) { bind =>
      val dir = bind(new MMapDir(t.fileDir))

      val sparse = new SparseIoBitsType[String]()
      val dense = new DenseIoBitsType[String]()

      // 1 billion
      val density = 64
      val sparsity = 16 * 1024
      val sparsity2 = size / 1024 // 1024 samples

      t.t("creating sparse bits of size " + size + "...")
      val s = {
        val bits = (0 until size).filter(_ % sparsity == 0).map(_.toLong)
        t.tUsLn(
          bind(
            sparse.create(
              dir.ref("sparse"),
              new SparseBits(bits, size))))
      }

      t.t("creating really sparse bits of size " + size + "...")
      val s2 = {
        val bits = (0 until size).filter(_ % sparsity2 == 0).map(_.toLong)
        t.tUsLn(
          bind(
            sparse.create(
              dir.ref("sparse2"),
              new SparseBits(bits, size))))
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
            dense.create(
              dir.ref("dense"),
              new DenseBits(b, size))))
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



  def makeBits[IoId](create:Bits => IoBits[IoId],
                     p:Double,
                     n:Long,
                     rnd:Random) = {
    create(
      new SparseBits((0L until n).filter(i => rnd.nextDouble() < p), n))
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

  def tTestPerf[IoId](t:TestTool, a:IoBits[IoId]) = {
    using(IoScope.open) { implicit scope =>
      implicit val io = IoContext()
      tPerOpUs(t, "  ", "popcount", a f)
      tPerOpUs(t, "  ", "negation", a~)
    }
  }

  def tTestPerf[IoId](t:TestTool, a:IoBits[IoId], b:IoBits[IoId]) = {
    using(IoScope.open) { implicit scope =>
      implicit val io = IoContext()
      tPerOpUs(t, "    ", "fAnd", a fAnd b)
      tPerOpUs(t, "    ", "&", a & b)
      tPerOpUs(t, "    ", "&~", a &~ b)
      tPerOpUs(t, "    ", "merge", a merge b)
    }
  }

  test("perf-by-sparsity") { t =>
    RefCounted.trace {
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
    RefCounted.trace {
      using (IoScope.open) { implicit scope =>
        val rnd = new Random(0)
        implicit val io = IoContext()
        t.t("creating multibits..")
        val n = 1024*1024
        val parts = 32
        val mbits =
          t.iMsLn(
            Array(4, 8, 12).map(s => Math.pow(0.5, s)).map { p =>
              (p, new MultiIoBits[Int](
                (0 until parts).map(i => makeBits(v => IoBits(v), p, n / parts, rnd)).toArray
              ))
            })
        t.t("creating bits of different sparseness..")
        val bits =
          t.iMsLn(
            Array(4, 8, 12).map(s => Math.pow(0.5, s)).map { p =>
              (p, makeBits(v => IoBits(v), p, n, rnd))
            })

        val all = mbits ++ bits

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
