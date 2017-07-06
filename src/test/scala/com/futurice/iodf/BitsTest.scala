package com.futurice.iodf

import java.io.{DataOutputStream, File}

import com.futurice.testtoys.{TestSuite, TestTool}
import com.futurice.iodf.store.{MMapDir, RamDir}
import com.futurice.iodf.Utils._
import com.futurice.iodf.ioseq._
import com.futurice.iodf.util.{LBits, LSeq, MultiBits, Tracing}

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
      val tp = getter(tested)
      val gp = getter(ground)
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
      implicit val io = IoContext()
      tAssertEquals[LBits](t, ground, tested, verbose)(
        ("(0)", _.apply(0)),
        (s"(${ground.size})", _.apply(ground.size-1)),
        (s"(${ground.size/2})", _.apply(ground.size/2)),
        ("n", _.n),
        ("f", _.f),
        ("count(b=>b)", _.count(b=>b)),
        ("iterator.count(b=>b)", _.iterator.count(b=>b)),
        ("trues.size", _.trues.size),
        ("trues.iterator.size", _.trues.iterator.size),
        ("(0 until size).count(b(_))", b => (0L until b.n).count(b(_))),
        ("~", _.~.toArray.toSeq),
        ("~.f", _.~.f),
        ("iterator", _.iterator.toArray.toSeq),
        ("leLongs", { b =>
          val l = b.leLongs.toArray.toSeq
          (l.size, l)
        }),
        ("trues", _.trues.toArray.toSeq),
        ("trues.iterator", _.trues.iterator.toArray.toSeq),
        ("trues.iterator.head", _.trues.iterator.head),
        ("trues.iterator.headOption", _.trues.iterator.headOption),
        ("trues.iterator.seeked(10)", {
          _.trues.iterator.seeked(10).toArray.toSeq
        }),
        ("trues.iterator.seeked(100)", {
          _.trues.iterator.seeked(100).toArray.toSeq
        }),
        ("trues.iterator.seeked(1000)", {
          _.trues.iterator.seeked(1000).toArray.toSeq
        }),
        (f"trues.iterator.take(8) + .copy.seeked(200).take(8)", { b =>
          val i = b.trues.iterator
          val copy = i.copy
          (i.take(8).toArray.toSeq, copy.seeked(200).take(8).toArray.toSeq)
        }),
        (f"views($x,$y)(0)", { b =>
          b.view(x, y).apply(0)
        }),
        (f"views($x,$y)(${y-x-1})", { b =>
          b.view(x, y).apply(y-x-1)
        }),
        (f"views($x,$y)(${(y-x)/2})", { b =>
          b.view(x, y).apply((y-x)/2)
        }),
        (f"views($x,$y).trues.headOption", { b =>
          b.view(x, y).trues.headOption
        }),
        (f"views($x,$y)", { b =>
          b.view(x, y).toArray.toSeq
        }),
        (f"views($x,$y).f", { b =>
          b.view(x, y).f
        }),
        (f"views($x,$y).leLongs", { b =>
          b.view(x, y).leLongs.toArray.toSeq
        }),
        (f"views($x,$y).trues", { b =>
          b.view(x, y).trues.toArray.toSeq
        }),
        (f"views($x,$y).trues.iterator.seeked(${x})", { b =>
          b.view(x, y).trues.iterator.seeked(x).toArray.toSeq
        }),
        (f"views($x,$y).views(${x/2},$x).f", { b =>
          b.view(x, y).view(x/2, x).f
        })
      )
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
      implicit val io = IoContext()
      tAssertEquals[(LBits, LBits)](t, (groundA, groundB), (testA, testB), verbose) (
        ("fAnd", { case (a, b) => a fAnd b }),
        ("fAndSparseSparse", { case (a, b) => LBits.fAndSparseSparse(a, b) }),
        ("fAndDenseSparse", { case (a, b) => LBits.fAndSparseDense(b, a) }),
        ("fAndSparseDense", { case (a, b) => LBits.fAndSparseDense(a, b) }),
        ("fAndDenseDense", { case (a, b) => LBits.fAndDenseDense(a, b) }),
        ("&.f", { case (a, b) => (a & b).f }),
        ("&", { case (a, b) => (a & b).toArray.toSeq }),
        ("&~", { case (a, b) => (a &~ b).toArray.toSeq }),
        ("&~.f", { case (a, b) => (a &~ b).f} ),
        ("& ~", { case (a, b) => (a & b.~).toArray.toSeq} ),
        ("& ~.f", { case (a, b) => (a & b.~).f} ),
        (".f+.f", { case (a, b) => (a.f + b.f) }),
        ("++.f", { case (a, b) => (a merge b).f }),
        ("++", { case (a, b) => (a merge b).toArray.toSeq })
      )
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
    val bits = new java.util.BitSet(bs.size)
    bs.zipWithIndex.foreach { case (bit, index) =>
      bits.set(index, bit)
    }
    LBits(bits, bs.size)
  }
  def toSparseBits(bs:Seq[Boolean]) =
    LBits(bs.zipWithIndex.filter(_._1).map(_._2.toLong), bs.size.toLong)

  // test the double wrapped
  def toBooleanSparseBits(bs:Seq[Boolean]) =
    LBits(LBits(bs.zipWithIndex.filter(_._1).map(_._2.toLong), bs.size.toLong))

  def toDenseIoBits(bs:Seq[Boolean])(implicit scope:IoScope, io:IoContext) =
    io.bits.dense.openCreated(
      io.allocator,
      LBits(bs.zipWithIndex.filter(_._1).map(_._2.toLong), bs.size.toLong))

  def toSparseIoBits(bs:Seq[Boolean])(implicit scope:IoScope, io:IoContext) =
    io.bits.sparse.openCreated(
      io.allocator,
      LBits(bs.zipWithIndex.filter(_._1).map(_._2.toLong), bs.size.toLong))

  def toIoBits(bs:Seq[Boolean])(implicit scope:IoScope, io:IoContext) =
    io.bits.openCreated(
      io.allocator,
      LBits(bs.zipWithIndex.filter(_._1).map(_._2.toLong), bs.size.toLong))

  def toMultiBits(bs:Seq[Boolean])(implicit scope:IoScope, io:IoContext) =
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
  test("lbits-boolean-sparse") { t =>
    tTestBits(t, toBooleanBits)
  }

  test("iobits-dense") { t =>
    Tracing.trace {
      scoped { implicit scope =>
        implicit val io = IoContext()
        tTestBits(t, toDenseIoBits(_))
      }
    }
  }

  test("iobits-sparse") { t =>
    Tracing.trace {
      scoped { implicit scope =>
        implicit val io = IoContext()
        tTestBits(t, toSparseIoBits(_))
      }
    }
  }

  test("iobits") { t =>
    Tracing.trace {
      scoped { implicit scope =>
        implicit val io = IoContext()
        tTestBits(t, toIoBits(_))
      }
    }
  }

  test("multibits") { t =>
    Tracing.trace {
      scoped { implicit scope =>
        implicit val io = IoContext()
        tTestBits(t, toMultiBits(_), verbose = true)
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
/*  test("lbits-sparseio-ops") { t =>
    Tracing.trace {
      scoped { implicit scope =>
        implicit val io = IoContext()
          tTestBitSOps(
            t,
            ((bs: Seq[Boolean]) => toSparseIoBits(bs)),
            ((bs: Seq[Boolean]) => toSparseIoBits(bs)),
            verbose = true)
      }
    }
  }*/

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
    Tracing.trace {
      scoped { implicit scope =>
        implicit val io = IoContext()
        tTestBitsOpsWith(t)(
          "boolean" -> toBooleanBits,
          "dense" -> toDenseBits,
          "sparse" -> toSparseBits,
          "denseIo" -> ((bs: Seq[Boolean]) => toDenseIoBits(bs)),
          "sparseIo" -> ((bs: Seq[Boolean]) => toSparseIoBits(bs)),
          "io" -> ((bs: Seq[Boolean]) => toIoBits(bs)),
          "multi" -> ((bs:Seq[Boolean]) => toMultiBits(bs)))
      }
    }
  }

}
