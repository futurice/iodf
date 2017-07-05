package com.futurice.iodf

import com.futurice.iodf.Utils.using
import com.futurice.iodf.ml.Knn
import com.futurice.iodf.perf.{BitsPerf, DfPerf, SeqPerf}
import com.futurice.iodf.store.{MMapDir}
import com.futurice.testtoys.TestRunner

object Tests {
  def main(args: Array[String]): Unit = {
    TestRunner(
      "io/test",
      Seq(
        new UtilsTest,
        new SeqTest,
        new SeqPerf,
        new BitsTest,
        new BitsPerf,
        new DfTest,
        new DfPerf,
        new MlTest))
      .exec(args)
  }

}