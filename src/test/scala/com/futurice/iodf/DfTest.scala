package com.futurice.iodf

import java.io.{DataOutputStream, File}
import java.util

import com.futurice.testtoys.{TestSuite, TestTool}
import com.futurice.iodf.store.{MMapDir, RamDir, RefCounted}
import com.futurice.iodf.Utils._
import com.futurice.iodf.ioseq._
import com.futurice.iodf.utils.{DenseBits, SparseBits}

import scala.reflect.runtime.universe._
import scala.util.Random
import scala.concurrent.duration._
import scala.concurrent.Await

case class ExampleItem( name:String, property:Boolean, quantity:Int, text:String, bigNumber:Long )


object ExampleItem {

  val letters = "abcdefghijklmnopqrstuvxyz"

  def nextLetter(rnd:Random) = letters.charAt(rnd.nextInt(letters.size))

  def apply(rnd:Random) : ExampleItem = {
    ExampleItem(nextLetter(rnd).toString,
      rnd.nextBoolean(),
      (rnd.nextGaussian() * 5).toInt,
      (0 until rnd.nextInt(4)).map(e => (0 until (1 + rnd.nextInt(2))).map(e => nextLetter(rnd)).mkString).mkString(" "),
      (rnd.nextGaussian() * 5).toLong * 1000000000L)
  }

  def makeItems(seed: Int, sz:Int) = {
    val rnd = new Random(seed)
    (0 until sz).map { i => ExampleItem(rnd) }
  }

}

/*object Foobar {
  def getInnerType[T](list:List[T])(implicit tag:TypeTag[T]) = tag.tpe.toString
  val stringList: List[String] = List("A")
  val stringName = getInnerType(stringList)

}
*/
/**
  * Created by arau on 24.11.2016.
  */
class DfTest extends TestSuite("df") {

  def tRefCount(t: TestTool) = {
    t.tln(RefCounted.openRefs + " refs open.")
  }

  def tDir(t: TestTool, dir: File): Unit = {
    t.tln((dir.listFiles().map(_.length()).sum / 1024) + "KB in " + dir.getName + ":")
    dir.listFiles().sorted.foreach { f =>
      t.tln(f"  ${(f.length() / 1024) + "KB"}%-6s ${f.getName}")

    }
  }

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

        val sparse = bits.create(dir.ref("bits3"), new SparseBits(Seq(2L, 445L), 1024))

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

/*  test("bits-B-perf") { t =>
    testBitPerf(t, 1000000000)
  }*/



  val items =
    Seq(ExampleItem("a", true, 3, "some text", 1000000000L),
        ExampleItem("b", false, 2, "more text", 1L),
        ExampleItem("c", true, 4, "even more text", 324234L))
  val indexConf =
    IndexConf[String]().withAnalyzer("text", e => e.asInstanceOf[String].split(" ").toSeq)

  test("creation") { t =>
    RefCounted.trace {
      val dfs = Dfs.fs
      using(new MMapDir(t.fileDir)) { dir =>
        using(dfs.createTypedDf[ExampleItem](items, dir)) { df =>
          t.tln
          t.tln("fields are: ")
          df.fieldNames.foreach { id => t.tln("  " + id) }
          t.tln
          t.tln("field values are: ")
          df.fieldIndexes.map { i =>
            using(df.openCol[Any](i)) { col =>
              t.tln("  " + col.map { b : Any =>
                b.toString
              }.mkString(","))
            }
          }
          t.tln
          t.tln("item 0 name is " + df("name", 0))
          t.tln("item 1 quantity is " + df("quantity", 1))
          t.tln("item 2 is " + df(2))
        }
        t.tln
        t.tln("db closed")
        t.tln("db reopened")
        using(dfs.openTypedDf[ExampleItem](dir)) { df =>
          t.tln
          t.tln("colIds are: ")
          df.colIds.foreach { id => t.tln("  " + id) }
          t.tln
          t.tln("columns are: ")
          (0 until df.colCount).map { i =>
            using(df.openCol[Any](i)) { col =>
              t.tln("  " + col.map { b : Any =>
                b.toString
              }.mkString(","))
            }
          }
          t.tln
          t.tln("item 0 name is " + df("name", 0))
          t.tln("item 1 quantity is " + df("quantity", 1))
          t.tln("item 2 is " + df(2))
        }
      }
    }
    t.tln
    tRefCount(t)
  }

  test("index") { t =>
    RefCounted.trace {

      def tDb(df: Df[String, String], index: Df[String, (String, Any)]) = {
        t.tln
        t.tln("colIds are: ")
        t.tln
        index.colIds.foreach {
          id => t.tln("  " + id)
        }

        t.tln
        t.tln("id indexes:")
        t.tln
        t.tln("  name->b         : " + index.indexOf("name" -> "b"))
        t.tln("  property->false : " + index.indexOf("property" -> false))
        t.tln("  quantity->4     : " + index.indexOf("quantity" -> 4))
        t.tln
        t.tln("columns are: ")
        t.tln
        (0 until index.colCount).map { i =>
          using(index.openCol[Any](i)) { col =>
            t.tln(f"  ${index.colIds(i)}%16s: " + col.map { b : Any =>
              b.toString
            }.mkString(","))
          }
        }
      }

      using(new MMapDir(new File(t.fileDir, "db"))) { dir =>
        using(new MMapDir(new File(t.fileDir, "idx"))) { dir2 =>
          val dfs = Dfs.fs
          using(dfs.createTypedDf[ExampleItem](items, dir)) { df =>
            using(dfs.createIndex(df, dir2, indexConf)) { index =>
              tDb(df, index)
            }
          }
          t.tln
          t.tln("db and index closed")
          t.tln("db and index reopened")
          using(dfs.openTypedDf[ExampleItem](dir)) { df =>
            using(dfs.openIndex(df, dir2)) { index =>
              tDb(df, index)
            }
          }
        }
      }
    }
    t.tln
    tRefCount(t)
  }

  def tIndexedDfPerf[IoId, T](t:TestTool, view:IndexedDf[IoId, T]) = {
    val df = view.df
    val index = view.indexDf

    t.tln
    val b = items.map(i => if (i.property) 1 else 0).sum
    val b2 = view.f("property" -> true)
    t.tln(f"property frequencies: original $b vs index $b2")

    val n = items.map(i => if (i.name == "h") 1 else 0).sum
    val n2 = view.f("name" -> "h")
    t.tln(f"name=h frequencies: original $n vs index $n2")

    val ids = index.colIds.toArray
    val rnd = new Random(0)
    t.tln;
    {
      t.t("searching 1024 ids...")
      var sum = 0
      val n = 1024
      val (ms, _) = TestTool.ms {
        (0 until n).foreach { i =>
          sum += index.indexOf(ids(rnd.nextInt(ids.size)))
        }
      }
      t.iln(ms + " ms.")
      t.tln
      t.tln("  checksum:    " + sum)
      t.iln("  time/lookup: " + ((ms * 1000) / n) + " us")
      t.tln
    };
    {
      t.t("making 1024 cofreq calculations...")
      var fA, fB, fAB = 0L
      val n = 1024
      val (ms, _) = TestTool.ms {
        (0 until n).foreach { i =>
          val co =
            view.coStats(ids(rnd.nextInt(ids.size)),
              ids(rnd.nextInt(ids.size)))
          fA += co.fA
          fB += co.fB
          fAB += co.fAB
        }
      }
      t.iln(ms + " ms.")
      t.tln
      t.tln("  checksums:    " + fA + "/" + fA + "/" + fAB)
      t.iln("  time/freq:    " + ((ms * 1000) / n) + " us")
      t.tln

      using(IoScope.open) { implicit scope =>
        implicit val io = IoContext()
        var fs, fA, fB, fAB = 0L
        val n = 1024

        t.i("opening bitsets (no id lookup)...")
        val bits = t.iMsLn {
          (0 until n).map { i =>
            view.openIndex(rnd.nextInt(index.colCount))
          }
        }: Seq[IoBits[IoId]]
        try {
          t.tln
          t.i("counting freqs...")
          val (ms, _) = TestTool.ms {
            (0 until n).foreach { i =>
              fs += bits(i).f
            }
          }
          t.iln(ms + " ms.")
          t.tln
          t.tln("  checksum:     " + fs)
          t.iln("  time/freq:    " + ((ms * 1000) / n) + " us")

          t.tln
          t.i("comparing bits...")
          val (ms2, _) = TestTool.ms {
            (0 until n).foreach { i =>
              val bA = bits(i)
              val bB = bits(rnd.nextInt(bits.size))
              fA += bA.f
              fB += bB.f
              fAB += bA.fAnd(bB)
            }
          }
          t.iln(ms2 + " ms.")
          t.tln
          t.tln("  checksums:    " + fA + "/" + fB + "/" + fAB)
          t.iln("  time/freq:    " + ((ms2 * 1000) / n) + " us")
          t.tln
          t.i("making three way bit and comparisons...")
          var fABC = 0L
          fAB = 0
          val (ms3, _) = TestTool.ms {
            (0 until n).foreach { i =>
              val bA = bits(i)
              val bB = bits(rnd.nextInt(bits.size))

              using(bA createAnd bB) { bAB =>
                val bC = bits(rnd.nextInt(bits.size))
                fAB += bAB.f
                fABC += bC.fAnd(bAB)
              }
            }
          }
          t.iln(ms3 + " ms.")
          t.tln
          t.tln("  checksums:    " + fAB + "/" + fABC)
          t.iln("  time/freq:    " + ((ms3 * 1000) / n) + " us")
          t.tln
        } finally {
          bits.foreach {
            _.close
          }
        }
      }
    }
  }

  test("1024-entry-index") { t =>
    RefCounted.trace {
      val file = new File(t.fileDir, "df")
      val dfs = Dfs.fs

      val items = ExampleItem.makeItems(0, 1024)

      val (creationMs, _) =
        TestTool.ms(
          using(dfs.createIndexedDfFile[ExampleItem](items, file)) { df =>
            t.tln
            t.tln("index column id count: " + df.indexDf.colCount)
            t.tln
            t.tln("first column ids are: ")
            t.tln
            df.indexDf.colIds.take(8).foreach {
              id => t.tln("  " + id)
            }
            t.tln
            t.tln("first columns are: ")
            t.tln
            (0 until 8).map { i =>
              using(df.indexDf.openCol[Any](i)) { col =>
                t.tln("  " + col.take(8).map {
                  _.toString
                }.mkString(","))
              }
            }
          })

      t.tln
      t.iln("db and index created and closed in " + creationMs + " ms")

      t.tln
      tDir(t, t.fileDir)
      t.tln

      val before = System.currentTimeMillis()
      using(dfs.openIndexedDfFile[ExampleItem](file)) { view =>
        t.iln("db and index reopened in " + (System.currentTimeMillis() - before) + " ms")
        val df = view.df
        val index = view.indexDf

        t.tln
        t.t("sanity checking...")
        t.tMsLn(
          (0 until df.lsize.toInt).foreach { i =>
            if (items(i) != df(i)) {
              t.tln(f" at $i original item ${items(i)} != original ${df(i)}")
            }
          })

        tIndexedDfPerf(t, view)
      }

      tRefCount(t)
    }
  }


  def testWritingPerf(testName:String, writer:(Seq[ExampleItem], File, IndexConf[String])=>Unit) =
    test(testName) { t =>
    RefCounted.trace {
      using(IoScope.open) { implicit bind =>
        implicit val io = IoContext()

        t.tln("testing, how writing index behaves time & memory wise")
        t.tln

        def M = 1024*1024.0

        val res =
          Seq(15, 16, 17, 18).map { exp =>
            val scale = 1 << exp
            val file = new File(t.fileDir, "df")

            t.tln(f"indexing $scale random items:")
            val baseMem = Utils.memory

            t.t(f"  creating items..")
            val items = t.iMsLn(ExampleItem.makeItems(0, scale))


            t.t(f"  indexing items..")
            using (new MemoryMonitor(100)) { mem : MemoryMonitor=>
              val (ms, _) =
                TestTool.ms(
                  writer(items, file, IndexConf()))
              t.iln(f"$ms ms")

              val stats =
                Await.result(mem.finish, Duration.Inf)

              val diskKb = file.length / 1024

              t.iln(f"  mem base: ${baseMem/M}%.1f MB")
              t.iln(f"  mem MB:   ${(stats.min-baseMem)/M}%.1f<${(stats.mean-baseMem)/M}%.1f<${(stats.max-baseMem)/M}%.1f (n:${stats.samples})")
              t.iln(f"  disk:     $diskKb KB")
              t.tln

              (ms, baseMem, stats, diskKb)
            }
          }
        t.tln("scaling:")
        t.tln
        t.iln("  time ms:     " + res.map(_._1).mkString(","))
        t.iln("  mem MB mean: " + res.map(e => f"${(e._3.mean-e._2)/M}%.1f").mkString(","))
        t.iln("  mem MB max:  " + res.map(e => f"${(e._3.max-e._2)/M}%.1f").mkString(","))
        t.tln("  disk KB:     " + res.map(_._4).mkString(","))
        t.tln
      }
    }
  }

  testWritingPerf("writing-indexed-perf", Dfs.fs.writeIndexedDfFile)
  testWritingPerf("writing-typed-perf", (items, dir, index) => Dfs.fs.writeTypedDfFile(items, dir))

  test("merging") { t =>
    RefCounted.trace {
      using(IoScope.open) { implicit bind =>
        val io = IoContext()
        val dfs = Dfs.fs
        val dirA = bind(new MMapDir(new File(t.fileDir, "dbA")))
        val dirB = bind(new MMapDir(new File(t.fileDir, "dbB")))
        val dirM = bind(new MMapDir(new File(t.fileDir, "dbM")))

        val itemsA = ExampleItem.makeItems(0, 1000)
        val itemsB = ExampleItem.makeItems(1, 1000)

        t.t("creating dbA..")
        val dbA =
          t.iMsLn(
            bind(dfs.createIndexedDf(itemsA, dirA)))
        t.t("creating dbB..")
        val dbB =
          t.iMsLn(
            bind(dfs.createIndexedDf(itemsB, dirB)))
        t.t("merging dbs..")
        t.iMsLn(
          dfs.writeMergedIndexedDf(dbA, dbB, dirM))
        t.t("opening merged db..")
        val dbM =
          t.iMsLn(
            bind(dfs.openIndexedDf[ExampleItem](dirM)))

        t.tln
        t.tln("merged db size: " + dbM.lsize)
        t.tln("merged db columns: " + dbM.colIds.mkString(", "))
        t.tln("merged db index entry count: " + dbM.indexDf.colIds.lsize)
        t.tln
        t.tln("merged db content:")
        def findErrors[Id, T](col:IoSeq[Id, T], colA:IoSeq[Id,T], colB:IoSeq[Id, T]) = {
          (if (col.lsize != colA.lsize + colB.lsize)
            Seq(("SIZE", col.lsize, colA.lsize, colB.lsize))
          else Seq())++
            (0L until colA.lsize).filter { i =>
              col(i) != colA(i)
            }.map(i => ("A", i, col(i), colA(i))) ++
            (0L until colB.lsize).filter{ i =>
              col(colA.lsize + i) != colB(i)
            }.map(i => ("B", colA.lsize + i, col(colA.lsize+i), colB(i)))
        }
        dbM.colIds.foreach { id =>
          using (IoScope.open) { implicit bind =>
            val col = dbM.col[Any](id)
            val colA = dbA.col[Any](id)
            val colB = dbB.col[Any](id)
            t.tln(f"  $id values: ")
            t.tln(f"     M:${col.take(4).mkString(",")}..${col.takeRight(4).mkString(",")}")
            t.tln(f"     A:${colA.take(4).mkString(",")}..${colA.takeRight(4).mkString(",")}")
            t.tln(f"     B:${colB.take(4).mkString(",")}..${colB.takeRight(4).mkString(",")}")
            t.tln(f"     errors: " + findErrors(col, colA, colB))
          }
        }
        t.tln
        t.tln("merged db indexes:")

        (0L until dbM.indexDf.colIds.lsize).filter(_ % 32 == 0).take(16).foreach { index =>
          val id = dbM.indexDf.colIds(index)
          using (IoScope.open) { implicit bind =>
            val col = dbM.index(id)
            val colA = dbA.index(id)
            val colB = dbB.index(id)
            t.tln(f"  $id values: ")
            t.tln(f"     M:${col.take(4).mkString(",")}..${col.takeRight(4).mkString(",")}")
            t.tln(f"     A:${colA.take(4).mkString(",")}..${colA.takeRight(4).mkString(",")}")
            t.tln(f"     B:${colB.take(4).mkString(",")}..${colB.takeRight(4).mkString(",")}")
            val errors =
              (0L until colA.lsize).filter { i =>
                col(i) != colA(i)
              }.map(i => ("A", i, col(i), colA(i))) ++
              (0L until colB.lsize).filter{ i =>
                col(colA.lsize + i) != colB(i)
              }.map(i => ("B", colA.lsize + i, col(colA.lsize+i), colB(i)))
            t.tln(f"     typeM:   " + col.getClass)
            t.tln(f"     typeA:   " + colA.getClass)
            t.tln(f"     typeB:   " + colB.getClass)
            t.tln(f"     errors: " + findErrors(col, colA, colB).mkString(","))
          }
        }
        t.tln
        t.tln("check index rows:")
        val errs =
          (0L until dbM.indexDf.colIds.lsize).map { index =>
            val id = dbM.indexDf.colIds(index)
            val col = dbM.index(id)
            val colA = dbA.index(id)
            val colB = dbB.index(id)
            val err = findErrors(col, colA, colB)
            if (err.size != 0) {
              t.tln(f"  bad row: $index/$id")
              t.tln(f"  $id values: ")
              t.tln(f"     M:${col.take(4).mkString(",")}..${col.takeRight(4).mkString(",")}")
              t.tln(f"     A:${colA.take(4).mkString(",")}..${colA.takeRight(4).mkString(",")}")
              t.tln(f"     B:${colB.take(4).mkString(",")}..${colB.takeRight(4).mkString(",")}")
              val errors =
                (0L until colA.lsize).filter { i =>
                  col(i) != colA(i)
                }.map(i => ("A", i, col(i), colA(i))) ++
                  (0L until colB.lsize).filter{ i =>
                    col(colA.lsize + i) != colB(i)
                  }.map(i => ("B", colA.lsize + i, col(colA.lsize+i), colB(i)))
              t.tln(f"     typeM:   " + col.getClass)
              t.tln(f"     typeA:   " + colA.getClass)
              t.tln(f"     typeB:   " + colB.getClass)
              t.tln(f"     errors:  " + err.mkString(","))
              errors.size
            }
            err.size
          }.sum
        t.tln(f"$errs errors found.")
        t.tln
        tIndexedDfPerf(t, dbM)
      }
    }
  }

  test("multidf") { t =>
    RefCounted.trace {
      using(IoScope.open) { implicit bind =>
        val io = IoContext()
        val dfs = Dfs.fs
        val dirA = bind(new MMapDir(new File(t.fileDir, "dbA")))
        val dirB = bind(new MMapDir(new File(t.fileDir, "dbB")))
        val dirM = bind(new MMapDir(new File(t.fileDir, "dbM")))

        val itemsA = ExampleItem.makeItems(0, 1000)
        val itemsB = ExampleItem.makeItems(1, 1000)

        t.t("creating dbA..")
        val dbA =
          t.iMsLn(
            bind(dfs.createTypedDf(itemsA, dirA)))
        t.t("creating dbB..")
        val dbB =
          t.iMsLn(bind(dfs.createTypedDf(itemsB, dirB)))
        t.t("opening multi df..")
        val dbM =
          t.iMsLn(
            bind(dfs.multiTypedDf(Array(dbA, dbB))))

        t.tln
        t.tln("merged db size: " + dbM.lsize)
        t.tln("merged db columns: " + dbM.colIds.mkString(", "))
        t.tln
        t.tln("merged db content:")
        def findErrors[Id, T](col:IoSeq[Id, T], colA:IoSeq[Id,T], colB:IoSeq[Id, T]) = {
          (if (col.lsize != colA.lsize + colB.lsize)
            Seq(("SIZE", col.lsize, colA.lsize, colB.lsize))
          else Seq())++
            (0L until colA.lsize).filter { i =>
              col(i) != colA(i)
            }.map(i => ("A", i, col(i), colA(i))) ++
            (0L until colB.lsize).filter{ i =>
              col(colA.lsize + i) != colB(i)
            }.map(i => ("B", colA.lsize + i, col(colA.lsize+i), colB(i)))
        }
        dbM.colIds.foreach { id =>
          using (IoScope.open) { implicit bind =>
            val col = dbM.col[Any](id)
            val colA = dbA.col[Any](id)
            val colB = dbB.col[Any](id)
            t.tln(f"  $id values: ")
            t.tln(f"     M:${col.take(4).mkString(",")}..${col.takeRight(4).mkString(",")}")
            t.tln(f"     A:${colA.take(4).mkString(",")}..${colA.takeRight(4).mkString(",")}")
            t.tln(f"     B:${colB.take(4).mkString(",")}..${colB.takeRight(4).mkString(",")}")
            t.tln(f"     errors: " + findErrors(col, colA, colB))
          }
        }

        t.tln
      }
    }
  }

  test("indexed-multidf") { t =>
    RefCounted.trace {
      using(IoScope.open) { implicit bind =>
        val io = IoContext()
        val dfs = Dfs.fs
        val dirA = bind(new MMapDir(new File(t.fileDir, "dbA")))
        val dirB = bind(new MMapDir(new File(t.fileDir, "dbB")))
        val dirM = bind(new MMapDir(new File(t.fileDir, "dbM")))

        val itemsA = ExampleItem.makeItems(0, 1000)
        val itemsB = ExampleItem.makeItems(1, 1000)

        t.t("creating dbA..")
        val dbA =
          t.iMsLn(
            bind(dfs.createIndexedDf(itemsA, dirA)))
        t.t("creating dbB..")
        val dbB =
          t.iMsLn(bind(dfs.createIndexedDf(itemsB, dirB)))
        t.t("opening multi df..")
        val dbM =
          t.iMsLn(
            bind(dfs.multiIndexedDf(Array(dbA, dbB))))

        t.tln
        t.t("warming the data frame...")
        t.iMsLn(dbM.df.colIds.lsize)
        t.t("warming the index ...")
        t.iMsLn(dbM.indexDf.colIds.lsize)
        t.tln
        t.tln("merged db size: " + dbM.lsize)
        t.tln("merged db columns: " + dbM.colIds.mkString(", "))
        t.tln("merged db index entry count: " + dbM.indexDf.colIds.lsize)
        t.tln
        t.tln("merged db content:")
        def findErrors[Id, T](col:IoSeq[Id, T], colA:IoSeq[Id,T], colB:IoSeq[Id, T]) = {
          (if (col.lsize != colA.lsize + colB.lsize)
            Seq(("SIZE", col.lsize, colA.lsize, colB.lsize))
          else Seq())++
            (0L until colA.lsize).filter { i =>
              col(i) != colA(i)
            }.map(i => ("A", i, col(i), colA(i))) ++
            (0L until colB.lsize).filter{ i =>
              col(colA.lsize + i) != colB(i)
            }.map(i => ("B", colA.lsize + i, col(colA.lsize+i), colB(i)))
        }
        dbM.colIds.foreach { id =>
          using (IoScope.open) { implicit bind =>
            val col = dbM.col[Any](id)
            val colA = dbA.col[Any](id)
            val colB = dbB.col[Any](id)
            t.tln(f"  $id values: ")
            t.tln(f"     M:${col.take(4).mkString(",")}..${col.takeRight(4).mkString(",")}")
            t.tln(f"     A:${colA.take(4).mkString(",")}..${colA.takeRight(4).mkString(",")}")
            t.tln(f"     B:${colB.take(4).mkString(",")}..${colB.takeRight(4).mkString(",")}")
            t.tln(f"     errors: " + findErrors(col, colA, colB))
          }
        }

        t.tln
        t.tln("merged db indexes:")

        (0L until dbM.indexDf.colIds.lsize).filter(_ % 32 == 0).take(16).foreach { index =>
          val id = dbM.indexDf.colIds(index)
          using (IoScope.open) { implicit bind =>
            val col = dbM.index(id)
            val colA = dbA.index(id)
            val colB = dbB.index(id)
            t.tln(f"  $id values: ")
            t.tln(f"     M:${col.take(4).mkString(",")}..${col.takeRight(4).mkString(",")}")
            t.tln(f"     A:${colA.take(4).mkString(",")}..${colA.takeRight(4).mkString(",")}")
            t.tln(f"     B:${colB.take(4).mkString(",")}..${colB.takeRight(4).mkString(",")}")
            val errors =
              (0L until colA.lsize).filter { i =>
                col(i) != colA(i)
              }.map(i => ("A", i, col(i), colA(i))) ++
                (0L until colB.lsize).filter{ i =>
                  col(colA.lsize + i) != colB(i)
                }.map(i => ("B", colA.lsize + i, col(colA.lsize+i), colB(i)))
            t.tln(f"     typeM:   " + col.getClass)
            t.tln(f"     typeA:   " + colA.getClass)
            t.tln(f"     typeB:   " + colB.getClass)
            t.tln(f"     errors: " + findErrors(col, colA, colB).mkString(","))
          }
        }
        t.tln
        t.tln("check index rows:")
        val errs =
          (0L until dbM.indexDf.colIds.lsize).map { index =>
            val id = dbM.indexDf.colIds(index)
            val col = dbM.index(id)
            val colA = dbA.index(id)
            val colB = dbB.index(id)
            val err = findErrors(col, colA, colB)
            if (err.size != 0) {
              t.tln(f"  bad row: $index/$id")
              t.tln(f"  $id values: ")
              t.tln(f"     M:${col.take(4).mkString(",")}..${col.takeRight(4).mkString(",")}")
              t.tln(f"     A:${colA.take(4).mkString(",")}..${colA.takeRight(4).mkString(",")}")
              t.tln(f"     B:${colB.take(4).mkString(",")}..${colB.takeRight(4).mkString(",")}")
              val errors =
                (0L until colA.lsize).filter { i =>
                  col(i) != colA(i)
                }.map(i => ("A", i, col(i), colA(i))) ++
                  (0L until colB.lsize).filter{ i =>
                    col(colA.lsize + i) != colB(i)
                  }.map(i => ("B", colA.lsize + i, col(colA.lsize+i), colB(i)))
              t.tln(f"     typeM:   " + col.getClass)
              t.tln(f"     typeA:   " + colA.getClass)
              t.tln(f"     typeB:   " + colB.getClass)
              t.tln(f"     errors:  " + err.mkString(","))
              errors.size
            }
            err.size
          }.sum
        t.tln(f"$errs errors found.")
        t.tln
        tIndexedDfPerf(t, dbM)
      }
    }
  }
}