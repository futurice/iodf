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


/**
  * Put the tests here, which are too slow to run as sanity checks
  */
class DfPerf extends TestSuite("perf/df") {


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

}
