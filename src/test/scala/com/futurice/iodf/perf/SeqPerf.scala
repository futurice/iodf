package com.futurice.iodf.perf

import java.io.{DataOutputStream, File}

import com.futurice.iodf.store.MMapDir
import com.futurice.iodf.{Dfs}
import com.futurice.testtoys.{TestSuite, TestTool}
import com.futurice.iodf.Utils._
import com.futurice.iodf.ioseq.{IoSeq, SeqIoType}
import com.futurice.iodf.util.LSeq

import scala.util.Random

/**
  * Created by arau on 13.6.2017.
  */
class SeqPerf extends TestSuite("perf/seq") {

  def tWritePerf[T](t:TestTool,
                    scale:Int,
                    source:Iterable[T],
                    ty:SeqIoType[T, LSeq[T],  _ <: IoSeq[T]]): Unit =
    scoped { implicit bind =>
    val dirFile = t.file("files")

    using (new MMapDir(dirFile)) { dir =>
      def writeN(n: Int) = {
        val file = dir.ref(n.toString)
        t.t("creating data..")
        val data = t.iMsLn(LSeq(source.take(n).toSeq))
        val (ms, _) =
          using (new DataOutputStream(file.create)) { out =>
            TestTool.ms(
              ty.write(out, data))
          }
        val kbs = new File(dirFile, n.toString).length() / 1024
        t.iln(f"writing $n entries took $ms ms")
        t.tln(f"resulting file is ${kbs} KB")
        t.iln(f"${n/ms.toDouble}%.3f entries/ms")
        t.iln(f"${kbs / ms.toDouble}%.3f KB/ms")
        t.tln
        (ms, kbs)
      }
      val results =
        (0 until 8).map { round => scale * (1<<round) }.map { n =>
          (n, writeN(n))
        }
    }
  }

  def stringSource(rnd:Random) = new Iterable[String] {
    def iterator =
      new Iterator[String] {
        def hasNext = true
        def next = (0 until rnd.nextInt(2)+1).map { i =>
          (0 until (rnd.nextInt(2) + 2)).map { r =>
            rnd.nextPrintableChar()
          }.mkString("")
        }.mkString(" ")
      }
  }
  def intSource(rnd:Random) = new Iterable[Int] {
    def iterator =
      new Iterator[Int] {
        def hasNext = true
        def next = rnd.nextInt()
      }
    }

  test("string") { t =>
    tWritePerf(
      t,
      1024,
      stringSource(new Random(0)),
      Dfs.fs.types.seqTypeOf[String])
  }

  test("int") { t =>
    tWritePerf(
      t,
      4*1024,
      intSource(new Random(0)),
      Dfs.fs.types.seqTypeOf[Int])
  }

}
