package com.futurice.iodf

import java.io.{DataOutputStream, File}
import java.util

import com.futurice.testtoys.{TestSuite, TestTool}
import com.futurice.iodf.store.{MMapDir, RamDir, RefCounted}
import com.futurice.iodf.Utils._
import com.futurice.iodf.ioseq._
import com.futurice.iodf.utils.LBits

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
      var sum = 0L
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
        }: Seq[LBits]
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
          dfs.writeMergedIndexedDf(Seq(dbA, dbB), dirM))
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
        def findErrors[Id, T](col:LSeq[T], colA:LSeq[T], colB:LSeq[T]) = {
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
            t.tln(f"     errors:  " + findErrors(col, colA, colB).mkString(","))
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
          t.iMsLn(dfs.createTypedDf(itemsA, dirA))
        t.t("creating dbB..")
        val dbB =
          t.iMsLn(dfs.createTypedDf(itemsB, dirB))
        t.t("opening multi df..")
        val dbM =
          t.iMsLn(
            bind(dfs.multiTypedDf(Array(dbA, dbB))))

        t.tln
        t.tln("merged db size: " + dbM.lsize)
        t.tln("merged db columns: " + dbM.colIds.mkString(", "))
        t.tln
        t.tln("merged db content:")
        def findErrors[Id, T](col:LSeq[T], colA:LSeq[T], colB:LSeq[T]) = {
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
            dfs.createIndexedDf(itemsA, dirA))
        t.t("creating dbB..")
        val dbB =
          t.iMsLn(dfs.createIndexedDf(itemsB, dirB))
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
        def findErrors[Id, T](col:LSeq[T], colA:LSeq[T], colB:LSeq[T]) = {
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
            t.tln(f"     errors:  " + findErrors(col, colA, colB).mkString(","))
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