package com.futurice.iodf.perf

import scala.reflect.runtime.universe._
import java.io.File

import com.futurice.iodf.Utils._
import com.futurice.iodf._
import com.futurice.iodf.df.{MultiCols, _}
import com.futurice.iodf.io.MergeableIoType
import com.futurice.iodf.providers.OrderingProvider
import com.futurice.iodf.store.{AllocateOnce, MMapDir, MMapFile}
import com.futurice.iodf.util.{LBits, Ref, Tracing}
import com.futurice.testtoys.{TestSuite, TestTool}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.Random


/**
  * Put the tests here, which are too slow to run as sanity checks
  */
class DfPerf extends TestSuite("perf/df") {

  def makeIoContext(implicit scope:IoScope) = {
    IoContext().withType[ExampleItem]
  }


  def tIndexedObjectsPerf[IoId, T](t:TestTool, view:IndexedObjects[T])(implicit io:IoContext) = {
    val df = view.df
    val index = view.indexDf

    t.tln

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
        var fs, fA, fB, fAB = 0L
        val n = 1024

        t.i("opening bitsets (no id lookup)...")
        val bits = t.iMsLn {
          (0 until n).map { i =>
            view.openIndex(rnd.nextInt(index.colCount.toInt))
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

  test("multidf-colidmemratio") { t =>
    t.t(f"  creating items..")
    Tracing.lightTrace {
      using(IoScope.open) { implicit bind =>
        implicit val io = makeIoContext
        val dir = MMapDir(t.fileDir)

        t.t("creating segments..")
        val dfFiles =
          t.iMsLn(
            (0 until 16).map { i =>
              val items = ExampleItem.makeItems(i, 16* 1024)
              val rf = dir.ref(f"df$i")
              rf.save(IndexedObjects.from(items, ExampleItem.indexConf))
            })
        t.tln
        val M = 1024*1024

        implicit val ord = Index.indexColIdOrdering[String]

        (0 until 2).foreach { scale =>
          (0 until 2).foreach { colIdMemRatioScale =>
            using(IoScope.open) { implicit bind =>

              val colIdMemRatio = 1 << (colIdMemRatioScale * 4)
              val segments = dfFiles.take(1 << (scale*2))

              t.tln("colIdMemRatio: " + colIdMemRatio)
              t.tln("segments:      " + segments.size + ", " + (segments.map(_.byteSize).sum / 1024) + " KB")

              val baseMem = Utils.memory

              t.t("opening multidf..")
              val (stats, multiDf) =
                t.iMsLn(
                  using(new MemoryMonitor(100)) { mem: MemoryMonitor =>
                    val dfs = dfFiles.map(f => Ref(f.openAs[IndexedObjects[ExampleItem]]))
                    val objs =
                      Objects[ExampleItem](
                        MultiCols.donate(dfs.map(e => Ref.mock(e.get.df)), colIdMemRatio))
                    val index = Index(MultiCols.open(dfs.map(e => Ref.mock(e.get.indexDf)), colIdMemRatio))
                    val df =
                      bind(
                        IndexedObjects(
                          Indexed(objs, index)))
                    (Await.result(mem.finish, Duration.Inf), df)
                  })
              t.tln
              val postMem = Utils.memory

              t.iln(f"  mem base: ${baseMem / M}%.1f MB -> ${postMem / M}%.1f MB")
              t.iln(f"  mem MB:   ${(stats.min - baseMem) / M}%.1f<${(stats.mean - baseMem) / M}%.1f<${(stats.max - baseMem) / M}%.1f (n:${stats.samples})")
              t.tln
              t.tln(f"items:        " + multiDf.lsize )
              t.tln(f"colIds:       " + multiDf.colIds.lsize )
              t.tln(f"index.colIds: " + multiDf.indexDf.colIds.lsize )
              t.tln
              tIndexedObjectsPerf(t, multiDf)
            }
          }
        }
      }
    }
  }


  def testWritingPerf[T:TypeTag:ClassTag](testName:String, toDf:Seq[ExampleItem]=>T) =
    test(testName) { t =>
      Tracing.lightTrace {
        using(IoScope.open) { implicit bind =>
          implicit val io = makeIoContext

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
              t.t(f"  creating dataframe..")
              val df = t.iMsLn(toDf(items))

              t.t(f"  indexing items..")
              using (new MemoryMonitor(100)) { mem : MemoryMonitor=>
                val (ms, _) = TestTool.ms(MMapFile(file).save(df))
                val old = t.peekLong
                t.i(f"$ms ms")
                old match {
                  case Some(v) => t.iln(f" (was $v ms)")
                  case None => t.iln("")
                }
                val stats =
                  Await.result(mem.finish, Duration.Inf)

                val diskKb = file.length / 1024

                t.iln(f"  mem base: ${baseMem/M}%.1f MB")
                t.iln(f"  mem MB:   ${(stats.min-baseMem)/M}%.1f<${(stats.mean-baseMem)/M}%.1f<${(stats.max-baseMem)/M}%.1f (n:${stats.samples})")
                t.iln(f"  disk:     $diskKb KB")
                t.iln(f"  speed:    ${diskKb / ms} MB/s")
                t.tln

                (ms, baseMem, stats, diskKb)
              }
            }
          t.tln("scaling:")
          t.tln
          t.iln( "  time ms:     " + res.map(_._1).mkString(","))
          t.iln( "  mem MB mean: " + res.map(e => f"${(e._3.mean-e._2)/M}%.1f").mkString(","))
          t.iln( "  mem MB max:  " + res.map(e => f"${(e._3.max-e._2)/M}%.1f").mkString(","))
          t.tln( "  disk KB:     " + res.map(_._4).mkString(","))
          t.iln(f"  speed MB/s:  ${res.map(e => e._4 / e._1).mkString(", ")}")
          t.tln
        }
      }
    }

  testWritingPerf[Objects[ExampleItem]]("writing-typed-perf", items => Objects.from(items) )
  testWritingPerf[IndexedObjects[ExampleItem]]("writing-indexed-perf", items => IndexedObjects.from(items, ExampleItem.indexConf))

}
