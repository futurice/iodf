package com.futurice.iodf

import java.io.{DataOutputStream, File}
import java.util

import com.futurice.testtoys.{TestSuite, TestTool}
import com.futurice.iodf.store.{MMapDir, RamDir, RefCounted}
import com.futurice.iodf.Utils._
import com.futurice.iodf.ioseq._
import com.futurice.iodf.utils.{LBits, MultiBits}

import scala.reflect.runtime.universe._
import scala.util.Random
import scala.concurrent.duration._
import scala.concurrent.Await

/**
  * Created by arau on 7.6.2017.
  */
class BitsTest extends TestSuite("bits") {

  def tAssertEquals[T](t:TestTool, ground:T, tested:T, verbose:Boolean = false)(properties:(String, T => Any)*) = {
    properties.flatMap { case (name, getter) =>
      val gp = getter(ground)
      val tp = getter(tested)
      if (gp != tp) {
        t.fail
        t.tln(f"  testing $name failed. the properties are not equal!")
        t.tln(f"    ground: " + gp)
        t.tln(f"    test:   " + tp)
        t.tln
        Some(name)
      } else {
        if (verbose) {
          t.tln(f"  $name ground and test properties are equal:")
          t.tln(f"    ground: " + gp)
          t.tln(f"    test:   " + tp)
          t.tln
        }
        None
      }
    }
  }

  def tTestBitsAgainst(t:TestTool, ground:LBits, tested:LBits, verbose:Boolean = false): Seq[String] = {
    val x = (ground.size * 0.25).toLong
    val y = (ground.size * 0.75).toLong
    using (IoScope.open) { implicit scope =>
      using(IoContext.open) { implicit io =>
        tAssertEquals[LBits](t, ground, tested, verbose)(
          ("n", _.n),
          ("f", _.f),
          ("~", _.~.toSeq),
          ("iterator", _.iterator.toSeq),
          ("leLongs", _.leLongs.toSeq),
          ("trues", _.trues.toSeq),
          ("trues.iterator", _.trues.iterator.toSeq),
          ("trues.iterator.head", _.trues.iterator.head),
          ("trues.iterator.headOption", _.trues.iterator.headOption),
          ("trues.iterator.seeked(10)", {
            _.trues.iterator.seeked(10).toSeq
          }),
          ("trues.iterator.seeked(100)", {
            _.trues.iterator.seeked(100).toSeq
          }),
          ("trues.iterator.seeked(1000)", {
            _.trues.iterator.seeked(1000).toSeq
          }),
          (f"trues.iterator.take(8) + .copy.seeked(200).take(8)", { b =>
            val i = b.trues.iterator
            (i.take(8).toSeq, i.copy.seeked(200).take(8).toSeq)
          }),
          (f"views($x,$y)", { b =>
            b.view(x, y).toSeq
          }),
          (f"views($x,$y).f", { b =>
            b.view(x, y).f
          }),
          (f"views($x,$y).leLongs", { b =>
            b.view(x, y).leLongs.toSeq
          }),
          (f"views($x,$y).trues", { b =>
            b.view(x, y).trues.toSeq
          }),
          (f"views($x,$y).trues.iterator.seeked(${2*x})", { b =>
            b.view(x, y).trues.iterator.seeked(2*x).toSeq
          })
        )
      }
    }
  }

  def tTestBits(t:TestTool,
                create:Seq[Boolean] => LBits,
                rounds:Seq[Double] = (0 until 8).map(i => Math.pow(2, -i)),
                verbose:Boolean = false) = {
    rounds.zipWithIndex.map{ case (p, round) =>
      if (verbose) t.tln(f"round $round with p $p%.3f:\n")
      val rnd = new Random(round)
      val testData = (0 until rnd.nextInt(2048)).map(i => rnd.nextDouble() < p)

      val ground = LBits(testData)

      val rv = using(create(testData)) { b =>
        (round, tTestBitsAgainst(t, ground, b, verbose))
      }
      if (verbose) t.tln("")
      rv
    }.filter(_._2.size > 0) match {
      case v if v.size == 0 => t.tln("ok.")
      case v  =>
        v.foreach { case (round, errors) =>
          t.tln("round " + round + " failed when testing " + errors.mkString(","))
        }
    }
  }

  def tTestBitsOpsAgainstGround(t:TestTool,
                                groundA:LBits, groundB:LBits,
                                testA:LBits, testB:LBits,
                                verbose:Boolean = false): Seq[String] = {
    using (IoScope.open) { implicit scope =>
      using (IoContext.open) { implicit io =>
        tAssertEquals[(LBits, LBits)](t, (groundA, groundB), (testA, testB), verbose) (
          ("fAnd", { case (a, b) => a fAnd b }),
          ("fAndSparseSparse", { case (a, b) => LBits.fAndSparseSparse(a, b) }),
          ("fAndDenseSparse", { case (a, b) => LBits.fAndSparseDense(b, a) }),
          ("fAndSparseDense", { case (a, b) => LBits.fAndSparseDense(a, b) }),
          ("fAndDenseDense", { case (a, b) => LBits.fAndDenseDense(a, b) }),
          ("&.f", { case (a, b) => (a & b).f }),
          ("&", { case (a, b) => (a & b).toSeq }),
          ("&~", { case (a, b) => (a &~ b).toSeq }),
          (".f+.f", { case (a, b) => (a.f + b.f) }),
          ("++.f", { case (a, b) => (a merge b).f }),
          ("++", { case (a, b) => (a merge b).toSeq })
        )
      }
    }
  }


  def tTestBitSOps(t:TestTool,
                   createA:Seq[Boolean] => LBits,
                   createB:Seq[Boolean] => LBits,
                   rounds:Seq[Double] = (0 until 4).map(i => Math.pow(4, -i)),
                   verbose:Boolean = false) = {
    rounds.zipWithIndex.flatMap { case (pA, roundA) =>
      if (verbose) t.tln(f"round $roundA for A with p $pA%.3f:\n")
      rounds.zipWithIndex.map { case (pB, roundB) =>
        if (verbose) t.tln(f"round $roundB for B with p $pB%.3f:\n")
        val rnd = new Random(roundA + roundB)
        val sz = rnd.nextInt(2048)
        val testDataA = (0 until sz).map(i => rnd.nextDouble() < pA)
        val testDataB = (0 until sz).map(i => rnd.nextDouble() < pB)

        if (verbose) {
          t.tln("  a trues are " + testDataA.zipWithIndex.filter(_._1).map(_._2).mkString(", "))
          t.tln("  b trues are " + testDataB.zipWithIndex.filter(_._1).map(_._2).mkString(", "))
        }

        val groundA = LBits(testDataA)
        val groundB = LBits(testDataB)

        val rv = using(createA(testDataA)) { testA =>
          using(createB(testDataB)) { testB =>
            (roundA, roundB, tTestBitsOpsAgainstGround(t, groundA, groundB, testA, testB, verbose))
          }
        }
        if (verbose) t.tln("")
        rv
      }
    }.filter(_._3.size > 0) match {
    case v if v.size == 0 => t.tln("    ok.")
    case v =>
      v.foreach { case (roundA, roundB, errors) =>
        t.tln(f"    round $roundA/$roundB failed when testing " + errors.mkString(","))
      }
    }
  }

  def toBooleanBits(bs:Seq[Boolean]) = LBits(bs)
  def toDenseBits(bs:Seq[Boolean]) = {
    val bits = new util.BitSet(bs.size)
    bs.zipWithIndex.foreach { case (bit, index) =>
      bits.set(index, bit)
    }
    LBits(bits, bs.size)
  }
  def toSparseBits(bs:Seq[Boolean]) =
    LBits(bs.zipWithIndex.filter(_._1).map(_._2.toLong), bs.size.toLong)

  def toDenseIoBits[IoId](bs:Seq[Boolean])(implicit scope:IoScope, io:IoContext[IoId]) =
    io.bits.dense.create(
      io.dir.freeRef,
      LBits(bs.zipWithIndex.filter(_._1).map(_._2.toLong), bs.size.toLong))

  def toSparseIoBits[IoId](bs:Seq[Boolean])(implicit scope:IoScope, io:IoContext[IoId]) =
    io.bits.sparse.create(
      io.dir.freeRef,
      LBits(bs.zipWithIndex.filter(_._1).map(_._2.toLong), bs.size.toLong))

  def toIoBits[IoId](bs:Seq[Boolean])(implicit scope:IoScope, io:IoContext[IoId]) =
    io.bits.create(
      io.dir.freeRef,
      LBits(bs.zipWithIndex.filter(_._1).map(_._2.toLong), bs.size.toLong))

  def toMultiBits[IoId](bs:Seq[Boolean])(implicit scope:IoScope, io:IoContext[IoId]) =
    MultiBits(
      bs.grouped(179).map { toIoBits(_) }.toSeq )

  test("lbits-boolean") { t =>
    tTestBits(t, toBooleanBits, verbose = true)
  }
  test("lbits-dense") { t =>
    tTestBits(t, toDenseBits )
  }
  test("lbits-sparse") { t =>
    tTestBits(t, toSparseBits)
  }

  test("iobits-dense") { t =>
    RefCounted.trace {
      scoped { implicit scope =>
        implicit val io = IoContext()
        tTestBits(t, toDenseIoBits(_))
      }
    }
  }

  test("iobits-sparse") { t =>
    RefCounted.trace {
      scoped { implicit scope =>
        implicit val io = IoContext()
        tTestBits(t, toSparseIoBits(_))
      }
    }
  }

  test("iobits") { t =>
    RefCounted.trace {
      scoped { implicit scope =>
        implicit val io = IoContext()
        tTestBits(t, toIoBits(_))
      }
    }
  }

  test("multibits") { t =>
    RefCounted.trace {
      scoped { implicit scope =>
        implicit val io = IoContext()
        tTestBits(t, toMultiBits(_))
      }
    }
  }

  test("lbits-boolean-ops") { t =>
    tTestBitSOps(
      t,
      bs => LBits(bs),
      bs => LBits(bs),
      verbose = true)
  }

  def tTestBitsOpsWith(t:TestTool)(bits:(String, Seq[Boolean] => LBits)*): Unit = {
    for ((n1, b1) <- bits) {
      t.tln("first is " + n1)
      t.tln
      for ((n2, b2) <- bits) {
        t.tln("  second is " + n2)
        t.tln
        tTestBitSOps(
          t,
          b1,
          b2)
        t.tln
      }
    }
  }

  test("lbits-ops") { t =>
    tTestBitsOpsWith(t)(
      "boolean"->toBooleanBits,
      "dense"->toDenseBits,
      "sparse"->toSparseBits)
  }

  test("all-bits-ops") { t =>
    RefCounted.trace {
      scoped { implicit scope =>
        implicit val io = IoContext()
        tTestBitsOpsWith(t)(
          "boolean" -> toBooleanBits,
          "dense" -> toDenseBits,
          "sparse" -> toSparseBits,
          "denseIo" -> ((bs: Seq[Boolean]) => toDenseIoBits(bs)),
          "sparseIo" -> ((bs: Seq[Boolean]) => toSparseIoBits(bs)),
          "io" -> ((bs: Seq[Boolean]) => toIoBits(bs)))
      }
    }
  }

  test("sparse-bits") { t =>
    RefCounted.trace {
      using(new MMapDir(t.fileDir)) { dir =>
        val bits = new SparseIoBitsType[String]()
        using(bits.create(dir.ref("bits"), LBits(Seq(0L, 2L), 4))) { b =>

          t.tln("bit(3):        " + b(2));

          t.tln("bit count is   " + b.f + "/" + b.n)
          t.tln("true bits are: " + b.trues.mkString(", ") + " (" + b.trues.size + ")")
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
        val bits = new DenseIoBitsType[String]()

        val b1 = bind(bits.create(dir.ref("bits"), LBits(Seq(false, false, true, false, true))))
        val b2 = bind(bits.create(dir.ref("bits2"), LBits(Seq(false, true, true, false))))
        val b3 = bind(bits.create(dir.ref("bits3"), LBits((0 until 150).map(_ % 3 == 0))))

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

        using(new DataOutputStream(dir.ref("bitsM").openOutput)) { o =>
          bits.writeAnyMerged(o, Seq(b1, b2))
        }
        val bM = bind(bits.open(bind(dir.open("bitsM"))))

        t.tln("bM=b1+b1")
        tBits(bM)
        t.tln("errors: " + findErrors(bM, b1, b2).mkString(","))


        using(new DataOutputStream(dir.ref("bitsM2").openOutput)) { o =>
          bits.writeAnyMerged(o, Seq(b3, b1))
        }
        val bM2 = bind(bits.open(bind(dir.open("bitsM2"))))

        t.tln("bM2=b3+b1")
        tBits(bM2)
        t.tln("errors: " + findErrors(bM2, b3, b1).mkString(","))

        using(new DataOutputStream(dir.ref("bitsM3").openOutput)) { o =>
          bits.writeAnyMerged(o, Seq(b1, b3))
        }
        val bM3 = bind(bits.open(bind(dir.open("bitsM3"))))

        t.tln("bM3=b1+b3")
        tBits(bM3)
        t.tln("errors: " + findErrors(bM3, b1, b3).mkString(","))

        using(new DataOutputStream(dir.ref("bitsM4").openOutput)) { o =>
          bits.writeAnyMerged(o, Seq(b3, b3))
        }

        val bM4 = bind(bits.open(bind(dir.open("bitsM4"))))

        t.tln("bM4=b3+b3")
        tBits(bM4)
        t.tln("errors: " + findErrors(bM4, b3, b3).mkString(","))
      }
    }
  }

  test("bits") { t =>
    RefCounted.trace {
      scoped { implicit bind =>
        val dir = MMapDir(t.fileDir)
        val bits =
          new IoBitsType[String](
            new SparseIoBitsType[String](),
            new DenseIoBitsType[String]())
        t.tln("creating short bit vector:")
        val dense = bits(dir.ref("bits"), LBits(Seq(0L, 2L), 4))

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
        val dense2 = bits(dir.ref("bits2"), LBits((0L until 1024L).filter(i => i % 3 == 1), 1024))

        t.tln("  bit type:      " + dense2.unwrap.getClass.getName)

        t.tln("  bit(2):        " + dense2(2));

        t.tln("  bit count is   " + dense2.f + "/" + dense2.n)
        t.tln("  first trues:   " + dense2.trues.take(10).mkString(", ") + " (" + dense2.trues.size + ")")
        t.tln
        t.tln("creating very sparse bit vector:")

        val sparse =
          bind(
            bits.create(dir.ref("bits3"), LBits(Seq(2L, 445L), 1024)))

        t.tln("  bit type:      " + sparse.unwrap.getClass)

        t.tln("  bit(2):        " + sparse(2));

        t.tln("  bit count is   " + sparse.f + "/" + sparse.n)
        t.tln("  true bits are: " + sparse.trues.mkString(", ") + " (" + sparse.trues.size + ")")
        t.tln

        implicit val io = IoContext()

        def tBits(b: LBits) = {
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

        def tTest(bA: LBits, bB: LBits) = {
          t.tln("    merge:")
          val bM = (bA merge bB)
          tBits(bM)
          if (bA.size == bB.size) {
            t.tln("    and:")
            val bAnd = (bA & bB)
            tBits(bAnd)
          }
        }

        val comb = Seq[(String, LBits)]("short dense" -> dense,
          "long dense" -> dense2,
          "sparse" -> sparse,
          "empty" -> bits.defaultSeq(1024).get)

        for ((n1, b1) <- comb; (n2, b2) <- comb) {
          t.tln(f"  for $n1 and $n2:")
          tTest(b1, b2)
          t.tln
        }
      }
    }
  }


}
