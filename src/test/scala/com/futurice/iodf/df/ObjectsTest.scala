package com.futurice.iodf.df

import java.io.File

import com.futurice.iodf._

import com.futurice.iodf.Utils._
import com.futurice.iodf.store.MMapFile
import com.futurice.iodf.util.{LBits, LSeq, Tracing}
import com.futurice.iodf.{IoContext, IoScope}
import com.futurice.testtoys.{TestSuite, TestTool}

import scala.util.Random

case class ExampleItem(
  name:String,
  property:Boolean,
  quantity:Int,
  text:String,
  bigNumber:Long,
  optionalInt:Option[Int],
  optionalText:Option[String])


object ExampleItem {

  val letters = "abcdefghijklmnopqrstuvxyz"

  val indexConf =
    IndexConf[String]()
      .withAnalyzer("text", e => e.asInstanceOf[String].split(" ").toSeq)
      .withAnalyzer("optionalText", _ match  {
        case Some(t: String) => t.split(" ").toSeq ++ Seq(true)
        case None => Seq(false)
      })


  def nextLetter(rnd:Random) = letters.charAt(rnd.nextInt(letters.size))

  def apply(rnd:Random) : ExampleItem = {
    ExampleItem(nextLetter(rnd).toString,
      rnd.nextBoolean(),
      (rnd.nextGaussian() * 5).toInt,
      (0 until rnd.nextInt(4)).map(e => (0 until (1 + rnd.nextInt(2))).map(e => nextLetter(rnd)).mkString).mkString(" "),
      (rnd.nextGaussian() * 5).toLong * 1000000000L,
      if (rnd.nextBoolean())
        Some((rnd.nextGaussian() * 5).toInt)
      else
        None,
      if (rnd.nextBoolean())
        Some((0 until rnd.nextInt(4)).map(e => (0 until (1 + rnd.nextInt(2))).map(e => nextLetter(rnd)).mkString).mkString(" "))
      else
        None)
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
class ObjectsTest extends TestSuite("df/objects") {

  import TestUtil._

  def makeIoContext(implicit scope:IoScope) = {
    IoContext().withType[ExampleItem]
  }

  def tDir(t: TestTool, dir: File): Unit = {
    t.tln((dir.listFiles().map(_.length()).sum / 1024) + "KB in " + dir.getName + ":")
    dir.listFiles().sorted.foreach { f =>
      t.tln(f"  ${(f.length() / 1024) + "KB"}%-6s ${f.getName}")

    }
  }

  val items =
    Seq(ExampleItem("a", true, 3, "some text", 1000000000L, None, None),
        ExampleItem("b", false, 2, "more text", 1L, Some(4), None),
        ExampleItem("c", true, 4, "even more text", 324234L, None, Some("extra text")))
  val indexConf = ExampleItem.indexConf


  def tLookUpExampleDf(t:TestTool, df:Objects[ExampleItem]) = {
    t.tln("item 0 name is " + df("name", 0))
    t.tln("item 1 quantity is " + df("quantity", 1))
    t.tln("item 2 is " + df(2))
  }

  def tCompareSeq[T](t:TestTool, a:LSeq[T], b:LSeq[T]) = {
    var errors = 0
    if (a.lsize != b.lsize) {
      t.tln(f"  A lsize ${a.lsize} != B lsize ${b.lsize}")
      errors += 1
    }
    (0L until Math.min(a.lsize, b.lsize)).foreach { i =>
      if (a(i) != b(i)) {
        t.tln(f"    at $i A value ${a(i)} != B value ${b(i)}")
        errors += 1
      }
    }
    errors
  }

  def tCompareDfs[ColId](t:TestTool, dfA:Df[ColId], dfB:Df[ColId]) = {
    var diffs = 0
    diffs +=
      tCompareSeq(t, dfA.colIds, dfB.colIds)

    (0L until Math.min(dfA.colIds.lsize, dfB.colIds.lsize)).foreach { i =>
      val colId = dfA.colIds(i)
      scoped { implicit scope =>
        diffs += tCompareSeq(t, dfA.col(i), dfB.col(i))
      }
      scoped { implicit scope =>
        diffs += tCompareSeq(t, dfA.col(colId), dfB.col(colId))
      }
    }
    if (diffs >= 0) {
      t.tln(f"  $diffs differences found.")
    } else {
      t.tln("  ok.")
    }
    diffs
  }

  test("objects") { t =>
    Tracing.trace {
      scoped { implicit bind =>
        val file = MMapFile(new File(t.fileDir, "myDf"))
        implicit val io = makeIoContext

        val df = Objects[ExampleItem](items)
        t.tln
        tDf(t, df)
        tLookUpExampleDf(t, df)
        t.tln
        t.t("writing df..")
        t.iMsLn(file.save(df))
        t.tln
        t.t("opening df..")
        val df2 =
          t.iMsLn(file.as[Objects[ExampleItem]])
        tDf(t, df2)
        tLookUpExampleDf(t, df2)
      }
    }
    t.tln
    tRefCount(t)
  }

  test("indexed-objects") { t =>
    Tracing.trace {
      using(IoScope.open) { implicit bind =>

        implicit val io = makeIoContext

        t.t("creating indexed dataframe..")
        val df =
          t.iMsLn {
            bind(
              IndexedObjects(ExampleItem.makeItems(0, 16), indexConf))
          }

        tDf(t, df.df)
        tDf(t, df.indexDf)

        val file = MMapFile(t.file("indexed-df"))
        t.tln
        t.t("writing dataframe..")
        t.iMsLn {
          file.save(IndexedObjects(ExampleItem.makeItems(0, 16), indexConf))
        }
        t.tln
        t.t("opening dataframe..")
        val df2 =
          t.iMsLn {
            bind(io.openAs[IndexedObjects[ExampleItem]](file))
          }

        tDf(t, df2.df)
        tDf(t, df2.indexDf)
      }
    }
  }

  def tIndexedObjectsPerf[T](t:TestTool, objs:IndexedObjects[T]) = {
    val df = objs.df
    val index = objs.indexDf

    t.tln
    val b = items.map(i => if (i.property) 1 else 0).sum
    val b2 = objs.f("property" -> true)
    t.tln(f"  property frequencies: original $b vs index $b2")

    val n = items.map(i => if (i.name == "h") 1 else 0).sum
    val n2 = objs.f("name" -> "h")
    t.tln(f"  name=h frequencies: original $n vs index $n2")

    val ids = index.colIds.toArray
    val rnd = new Random(0)
    t.tln;
    {
      t.t("  searching 1024 ids...")
      var sum = 0L
      val n = 1024
      val (ms, _) = TestTool.ms {
        (0 until n).foreach { i =>
          sum += index.indexOf(ids(rnd.nextInt(ids.size)))
        }
      }
      t.iln(ms + " ms.")
      t.tln
      t.tln("    checksum:    " + sum)
      t.iln("    time/lookup: " + ((ms * 1000) / n) + " us")
      t.tln
    };
    {
      t.t("  making 1024 cofreq calculations...")
      var fA, fB, fAB = 0L
      val n = 1024
      val (ms, _) = TestTool.ms {
        (0 until n).foreach { i =>
          val co =
            objs.coStats(ids(rnd.nextInt(ids.size)),
              ids(rnd.nextInt(ids.size)))
          fA += co.fA
          fB += co.fB
          fAB += co.fAB
        }
      }
      t.iln(ms + " ms.")
      t.tln
      t.tln("    checksums:    " + fA + "/" + fA + "/" + fAB)
      t.iln("    time/freq:    " + ((ms * 1000) / n) + " us")
      t.tln

      using(IoScope.open) { implicit scope =>
        implicit val io = IoContext()
        var fs, fA, fB, fAB = 0L
        val n = 1024

        t.i("  opening bitsets (no id lookup)...")
        val bits = t.iMsLn {
          (0 until n).map { i =>
            objs.openIndex(rnd.nextInt(index.colCount.toInt))
          }
        }: Seq[LBits]
        try {
          t.tln
          t.i("  counting freqs...")
          val (ms, _) = TestTool.ms {
            (0 until n).foreach { i =>
              fs += bits(i).f
            }
          }
          t.iln(ms + " ms.")
          t.tln
          t.tln("    checksum:     " + fs)
          t.iln("    time/freq:    " + ((ms * 1000) / n) + " us")

          t.tln
          t.i("  comparing bits...")
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
          t.tln("    checksums:    " + fA + "/" + fB + "/" + fAB)
          t.iln("    time/freq:    " + ((ms2 * 1000) / n) + " us")
          t.tln
          t.i("  making three way bit and comparisons...")
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
          t.tln("    checksums:    " + fAB + "/" + fABC)
          t.iln("    time/freq:    " + ((ms3 * 1000) / n) + " us")
          t.tln
        } finally {
          bits.foreach {
            _.close
          }
        }
      }
    }
  }



  test("1024-indexed-objects") { t =>
//    Tracing.trace {
      scoped { implicit scoped =>
        implicit val io = makeIoContext

        val file = MMapFile(t.fileDir, "df")

        val items = ExampleItem.makeItems(0, 1024)

        t.t("creating heap df..")
        val heapDf =
          t.iMsLn(
            IndexedObjects[ExampleItem](items, ExampleItem.indexConf))
        t.tln
        t.t("creating io df..")
        val ioDf = t.iMsLn(file.saved(heapDf))
        t.tln
        t.tln("testing heap df:")
        t.tln

        tIndexedObjectsPerf(t, heapDf)
        t.tln
        t.tln("testing io df:")
        t.tln
        tIndexedObjectsPerf(t, ioDf)

        t.tln("comparing main dfs:")
        t.tln

        tCompareDfs(t, heapDf.df, ioDf.df)
        t.tln
        t.tln("comparing index dfs:")
        t.tln
        tCompareDfs(t, heapDf.indexDf, ioDf.indexDf)

        t.tln
      }
 //   }
  }

  /*

    test("index") { t =>
      Tracing.trace {

        def tDb(df: Df[String], index: Df[(String, Any)]) = {
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
            using(dfs.createTypedDf[ExampleItem, String](items, dir)) { df =>
              using(dfs.createIndex(df, dir2, indexConf)) { index =>
                tDb(df, index)
              }
            }
            t.tln
            t.tln("db and index closed")
            t.tln("db and index reopened")
            using(dfs.openTypedDf[ExampleItem, String](dir)) { df =>
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



    test("merging") { t =>
      Tracing.trace {
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
              bind(dfs.createIndexedObjects(itemsA, dirA)))
          t.t("creating dbB..")
          val dbB =
            t.iMsLn(
              bind(dfs.createIndexedObjects(itemsB, dirB)))
          t.t("merging dbs..")
          t.iMsLn(
            dfs.writeMergedIndexedObjects(Seq(dbA, dbB), dirM))
          t.t("opening merged db..")
          val dbM =
            t.iMsLn(
              bind(dfs.openIndexedObjects[ExampleItem, String](dirM)))

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
          tIndexedObjectsPerf(t, dbM)
        }
      }
    }

    test("multidf") { t =>
      Tracing.trace {
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

    test("empty-indexed-multidf") { t =>
      Tracing.trace {
        using(IoScope.open) { implicit bind =>
          val io = IoContext()
          val dfs = Dfs.fs

          t.t("opening multi df..")
          val dbM =
            t.iMsLn(
              bind(dfs.multiIndexedObjects[ExampleItem](Array(), 1)))

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
          dbM.colIds.foreach { id =>
            using (IoScope.open) { implicit bind =>
              val col = dbM.col[Any](id)
              t.tln(f"  $id values: ")
              t.tln(f"     " + col.mkString(", "))
            }
          }
          t.tln
          t.tln("col for text: " + dbM.col("text").mkString(", "))
          t.tln
          t.tln("merged db indexes:")
          dbM.indexDf.colIds.foreach { id =>
            t.tln(f"  " + id)
          }
          t.tln
          t.tln("index for (text->aa): " + dbM.index("text" -> "aa").mkString(", "))
          t.tln
        }
      }
    }
    test("indexed-multidf") { t =>
      Tracing.trace {
        using(IoScope.open) { implicit bind =>
          val io = IoContext()
          val dfs = Dfs.fs
          val dirA = bind(new MMapDir(new File(t.fileDir, "dbA")))
          val dirB = bind(new MMapDir(new File(t.fileDir, "dbB")))

          val itemsA = ExampleItem.makeItems(0, 1000)
          val itemsB = ExampleItem.makeItems(1, 1000)

          t.t("creating dbA..")
          val dbA =
            t.iMsLn(
              dfs.createIndexedObjects(itemsA, dirA))
          t.t("creating dbB..")
          val dbB =
            t.iMsLn(dfs.createIndexedObjects(itemsB, dirB))
          t.t("opening multi df..")
          val dbM =
            t.iMsLn(
              bind(dfs.multiIndexedObjects(Array(dbA, dbB), 1)))

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

          /*        implicit val ordering = dfs.indexColIdOrdering
                  val ground =
                    MergeSortIterator.apply[(String, Any)](Array(dbA.indexDf.colIds.iterator, dbB.indexDf.colIds.iterator)).toArray
                  val iterated = dbM.indexDf.colIds.iterator.toArray
                  val entries =
                    (0L until dbM.indexDf.colIds.lsize).map( i =>
                      dbM.indexDf.asInstanceOf[MultiDf[String, (String, Any)]].entryOfIndex(i))



                  t.tln("ground:   " + ground.mkString(","))
                  t.tln("test:     " + entries.mkString(","))*/
          /*        t.tln("iterated: " + iterated.mkString(","))
                  t.tln("applied:  " + applied.mkString(","))*/


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
          tIndexedObjectsPerf(t, dbM)
        }
      }
    }*/
}