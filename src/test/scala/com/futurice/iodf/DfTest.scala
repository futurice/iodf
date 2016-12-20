package com.futurice.iodf

import java.io.File

import com.futurice.testtoys.{TestSuite, TestTool}
import com.futurice.iodf.store.{MMapDir, RefCounted}
import com.futurice.iodf.Utils.using
import com.futurice.iodf.ioseq.{DenseIoBits, IoBits, SparseBits, SparseIoBitsType}

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

  def tRefCount(t:TestTool) = {
    t.tln(RefCounted.openRefs + " refs open.")
  }

  def tDir(t:TestTool, dir:File): Unit = {
    t.tln((dir.listFiles().map(_.length()).sum / 1024) + "KB in " + dir.getName + ":")
    dir.listFiles().sorted.foreach { f =>
      t.tln(f"  ${(f.length()/1024) + "KB"}%-6s ${f.getName}")

    }
  }

  test("bits") { t =>
    using(new MMapDir(t.fileDir)) { dir =>
      val bits = new SparseIoBitsType[String]()
      using (bits.create(dir.ref("bits"), new SparseBits(Seq(0L, 2L), 4))) { b =>

        t.tln("bit(3):        " + b(2));

        t.tln("bit count is   " + b.f + "/" + b.n)
        t.tln("true bits are: " + b.trues.mkString(", ") + " (" + b.trues.lsize + ")")
        t.tln("bits by index: " + (0L until b.n).map { b(_) }.mkString(", "))
        t.tln("bits by iter:  " + b.map { _.toString }.mkString(", "))
      }
    }
  }

  test("bits-perf") { t =>
    val sizes = Array(16, 256, 4*1024, 1024*1024)

    sizes.foreach { sz =>
      using(new MMapDir(t.fileDir)) { dir =>
        val bits = new SparseIoBitsType[String]()
        val data = (0 until sz).filter(_ % 4 == 0).map(_.toLong)

        t.t("creating sparse bits of size " + sz + "...")
        t.tMsLn(bits.create(dir.ref("bits"), new SparseBits(data, sz))).close()
      }
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

      def tDb(df:Df[String, String], index:Df[String, (String, Any)]) = {
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
                        (0 until rnd.nextInt(4)).map(e => (0 until (1+rnd.nextInt(2))).map(e => nextLetter).mkString).mkString(" "))
          }

        val (creationMs, _) =
          TestTool.ms(
            using(dfs.haveTypedDb(items, dir)) { db =>
              t.tln
              t.tln("index column id count: " + db.index.colCount)
              t.tln
              t.tln("first column ids are: ")
              t.tln
              db.index.colIds.take(8).foreach {
                id => t.tln("  " + id)
              }
              t.tln
              t.tln("first columns are: ")
              t.tln
              (0 until 8).map { i =>
                using(db.index.openCol[Any](i)) { col =>
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
          val index = view.index
          t.iln("db and index reopened in " + (System.currentTimeMillis() - before) + " ms")

          t.tln
          t.t("sanity checking...")
          t.tMsLn(
            (0 until df.lsize.toInt).foreach { i =>
              if (items(i) != df(i)) {
                t.tln(f" at $i original item ${items(i)} != original ${df(i)}" )
              }
            })

          t.tln
          val b = items.map(i => if (i.property) 1 else 0 ).sum
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
                  view.co(ids(rnd.nextInt(ids.size)),
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
                view.open(rnd.nextInt(index.colCount))
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
              t.tln("  checksums:    " + fA + "/" + fA + "/" + fAB)
              t.iln("  time/freq:    " + ((ms2 * 1000) / n) + " us")
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
    tRefCount(t)
  }

}
