package com.futurice.iodf

import com.futurice.iodf.Utils.using
import com.futurice.iodf.ml.Knn
import com.futurice.iodf.store.{MMapDir, RefCounted}
import com.futurice.testtoys.TestRunner

object Tests {
  def main(args: Array[String]): Unit = {
    TestRunner(
      "io/test",
      Seq(
        new DfTest,
        new MlTest))
      .exec(args)
  }

}