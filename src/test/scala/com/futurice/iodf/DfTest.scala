package com.futurice.iodf

import java.io.{DataOutputStream, File}
import java.util

import com.futurice.testtoys.{TestSuite, TestTool}
import com.futurice.iodf.store.{MMapDir, RamDir, RefCounted}
import com.futurice.iodf.Utils.using
import com.futurice.iodf.ioseq._

import scala.reflect.runtime.universe._
import scala.util.Random

case class ExampleItem( name:String, property:Boolean, quantity:Int, text:String )

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

  test("bits") { t =>
    using(new MMapDir(t.fileDir)) { dir =>
      val bits = new SparseIoBitsType[String]()
      using(bits.create(dir.ref("bits"), new SparseBits(Seq(0L, 2L), 4))) { b =>

        t.tln("bit(3):        " + b(2));

        t.tln("bit count is   " + b.f + "/" + b.n)
        t.tln("true bits are: " + b.trues.mkString(", ") + " (" + b.trues.lsize + ")")
        t.tln("bits by index: " + (0L until b.n).map {
          b(_)
        }.mkString(", "))
        t.tln("bits by iter:  " + b.map {
          _.toString
        }.mkString(", "))
      }
    }
  }

  test("dense-bits") { t =>
    using(IoScope.open) { implicit bind =>
      val dir = bind(new MMapDir(t.fileDir))
      val bits = new DenseIoBitsType[String]()

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

      def tBits(b:DenseIoBits[String]) {
        t.tln("bit(2):        " + b(2));

        t.tln("bit count is     " + b.f + "/" + b.n)
        t.tln("true bits length " + b.trues.size)
        t.tln("true bits are:   " + b.trues.mkString(", "))
        t.tln("real true bits:  " + b.zipWithIndex.filter(_._1).map(_._2).mkString(", "))
        t.tln("bits by index:   " + (0L until b.n).map {
          b(_)
        }.mkString(", "))
        t.tln("bits by iter:    " + b.map {
          _.toString
        }.mkString(", "))
        if (b.f != b.trues.size) throw new RuntimeException
        t.tln
      }
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

  test("bits-perf") { t =>
    val sizes = Array(16, 256, 4 * 1024, 1024 * 1024)

    sizes.foreach { sz =>
      using(new MMapDir(t.fileDir)) { dir =>
        val bits = new SparseIoBitsType[String]()
        val data = (0 until sz).filter(_ % 4 == 0).map(_.toLong)

        t.t("creating sparse bits of size " + sz + "...")
        t.tMsLn(bits.create(dir.ref("bits"), new SparseBits(data, sz))).close()
      }
    }
  }

  test("bits-B-perf") { t =>

    using(IoScope.open) { bind =>
      val dir = bind(new MMapDir(t.fileDir))

      val sparse = new SparseIoBitsType[String]()
      val dense = new DenseIoBitsType2[String]()

      val size = 1000000000
      // 1 billion
      val density = 64
      val sparsity = 16 * 1024
      val sparsity2 = size / 1024 // 1024 samples

      t.t("creating sparse bits of size " + size + "...")
      val s = {
        val bits = (0 until size).filter(_ % sparsity == 0).map(_.toLong)
        t.tMsLn(
          bind(
            sparse.create(
              dir.ref("sparse"),
              new SparseBits(bits, size))))
      }

      t.t("creating really sparse bits of size " + size + "...")
      val s2 = {
        val bits = (0 until size).filter(_ % sparsity2 == 0).map(_.toLong)
        t.tMsLn(
          bind(
            sparse.create(
              dir.ref("sparse2"),
              new SparseBits(bits, size))))
      }

      t.t("populating bitset of size " + size + "...")
      val b = t.tMsLn({
        val rv = new util.BitSet(size)
        (0 until size).filter(_ % density == 0).foreach(rv.set(_))
        rv
      })

      t.t("creating dense bits of size " + size + "...")
      val d =
        t.tMsLn(
          bind(
            dense.create(
              dir.ref("dense"),
              new DenseBits(b, size))))
      t.tln
      t.t("counting sparse f..")
      val sf = t.tMsLn(s.f)
      t.t("counting really sparse f..")
      val s2f = t.tMsLn(s2.f)
      t.t("counting dense f..")
      val df = t.tMsLn(d.f)
      t.t("counting bitset f..")
      val bf = t.tMsLn(b.cardinality())
      t.tln

      t.t("counting sparse & sparse..")
      val ss =
        t.tMsLn(
          s.fAnd(s))
      t.t("counting really sparse & really sparse..")
      val ss2 =
        t.tMsLn(
          s2.fAnd(s2))
      t.t("counting dense & dense..")
      val dd =
        t.tMsLn(
          d.fAnd(d))
      t.t("counting sparse & dense..")
      val sd =
        t.tMsLn(
          s.fAnd(d))
      t.t("counting really sparse & dense..")
      val sd2 =
        t.tMsLn(
          s2.fAnd(d))
      t.t("counting bitset & bitset..")
      val bb =
        t.tMsLn({
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


  val items =
    Seq(ExampleItem("a", true, 3, "some text"),
        ExampleItem("b", false, 2, "more text"),
        ExampleItem("c", true, 4, "even more text"))
  val indexConf =
    IndexConf[String]().withAnalyzer("text", e => e.asInstanceOf[String].split(" ").toSeq)

  test("creation") { t =>
    RefCounted.trace {
      val dfs = Dfs.default
      using(new MMapDir(t.fileDir)) { dir =>
        using(dfs.createTyped[ExampleItem](items, dir)) { df =>
          t.tln
          t.tln("fields are: ")
          df.fieldNames.foreach { id => t.tln("  " + id) }
          t.tln
          t.tln("field values are: ")
          df.fieldIndexes.map { i =>
            using(df.openCol[Any](i)) { col =>
              t.tln("  " + col.map {
                _.toString
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
        using(dfs.openTyped[ExampleItem](dir)) { df =>
          t.tln
          t.tln("colIds are: ")
          df.colIds.foreach { id => t.tln("  " + id) }
          t.tln
          t.tln("columns are: ")
          (0 until df.colCount).map { i =>
            using(df.openCol[Any](i)) { col =>
              t.tln("  " + col.map {
                _.toString
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
            t.tln(f"  ${index.colIds(i)}%16s: " + col.map {
              _.toString
            }.mkString(","))
          }
        }
      }

      using(new MMapDir(new File(t.fileDir, "db"))) { dir =>
        using(new MMapDir(new File(t.fileDir, "idx"))) { dir2 =>
          val dfs = Dfs.default
          using(dfs.createTyped[ExampleItem](items, dir)) { df =>
            using(dfs.createIndex(df, dir2, indexConf)) { index =>
              tDb(df, index)
            }
          }
          t.tln
          t.tln("db and index closed")
          t.tln("db and index reopened")
          using(dfs.openTyped[ExampleItem](dir)) { df =>
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

  test("1024-entry-index") { t =>
    RefCounted.trace {
      val dirFile = new File(t.fileDir, "db")
      using(RamDir()) { ram =>
        val ioBits =
          new IoBitsType(new SparseIoBitsType[Int](),
            new DenseIoBitsType[Int]())
        using(new MMapDir(dirFile)) { dir =>
          val dfs = Dfs.default

          val rnd = new Random(0)
          val letters = "abcdefghijklmnopqrstuvxyz"

          def nextLetter = letters.charAt(rnd.nextInt(letters.size))

          val items =
            (0 until 1024).map { i =>
              ExampleItem(nextLetter.toString,
                rnd.nextBoolean(),
                (rnd.nextGaussian() * 5).toInt,
                (0 until rnd.nextInt(4)).map(e => (0 until (1 + rnd.nextInt(2))).map(e => nextLetter).mkString).mkString(" "))
            }

          val (creationMs, _) =
            TestTool.ms(
              using(dfs.createTypedDb(items, dir)) { db =>
                t.tln
                t.tln("index column id count: " + db.indexDf.colCount)
                t.tln
                t.tln("first column ids are: ")
                t.tln
                db.indexDf.colIds.take(8).foreach {
                  id => t.tln("  " + id)
                }
                t.tln
                t.tln("first columns are: ")
                t.tln
                (0 until 8).map { i =>
                  using(db.indexDf.openCol[Any](i)) { col =>
                    t.tln("  " + col.take(8).map {
                      _.toString
                    }.mkString(","))
                  }
                }
              })

          t.tln
          t.iln("db and index created and closed in " + creationMs + " ms")
          val before = System.currentTimeMillis()

          t.tln
          tDir(t, dirFile)
          t.tln

          using(dfs.haveTypedDb(items, dir)) { view =>
            val df = view.df
            val index = view.indexDf
            t.iln("db and index reopened in " + (System.currentTimeMillis() - before) + " ms")

            t.tln
            t.t("sanity checking...")
            t.tMsLn(
              (0 until df.lsize.toInt).foreach { i =>
                if (items(i) != df(i)) {
                  t.tln(f" at $i original item ${items(i)} != original ${df(i)}")
                }
              })

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
          };
          {
            var fs, fA, fB, fAB = 0L
            val n = 1024

            t.i("opening bitsets (no id lookup)...")
            val bits = t.iMsLn {
              (0 until n).map { i =>
                view.openIndex(rnd.nextInt(index.colCount))
              }
            }: Seq[IoBits[String]]
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

                  using(ioBits.createAnd(ram, bA, bB)) { bAB =>
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
      }
    }
    tRefCount(t)
  }

  test("merging") { t =>
    RefCounted.trace {
      using(IoScope.open) { implicit bind =>
        val io = IoContext()
        val dfs = Dfs.default
        val dirA = bind(new MMapDir(new File(t.fileDir, "dbA")))
        val dirB = bind(new MMapDir(new File(t.fileDir, "dbB")))
        val dirM = bind(new MMapDir(new File(t.fileDir, "dbM")))

        val letters = "abcdefghijklmnopqrstuvxyz"

        def makeItems(seed: Int, sz:Int) = {
          val rnd = new Random(seed)

          def nextLetter = letters.charAt(rnd.nextInt(letters.size))

          (0 until sz).map { i =>
            ExampleItem(nextLetter.toString,
              rnd.nextBoolean(),
              (rnd.nextGaussian() * 5).toInt,
              (0 until rnd.nextInt(4)).map(e => (0 until (1 + rnd.nextInt(2))).map(e => nextLetter).mkString).mkString(" "))
          }
        }

        val itemsA = makeItems(0, 1000)
        val itemsB = makeItems(1, 1000)

        t.t("creating dbA..")
        val dbA =
          t.iMsLn(
            bind(dfs.createTypedDb(itemsA, dirA)))
        t.t("creating dbB..")
        val dbB =
          t.iMsLn(
            bind(dfs.createTypedDb(itemsB, dirB)))
        t.t("merging dbs..")
        t.iMsLn(
          dfs.writeMergedDb(dbA, dbB, dirM))
        t.t("opening merged db..")
        val dbM =
          t.iMsLn(
            bind(dfs.openTypedDb[ExampleItem](dirM)))

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
        (0L until dbM.indexDf.colIds.lsize).foreach { index =>
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
          }

        }

        t.tln
      }
    }
  }
}