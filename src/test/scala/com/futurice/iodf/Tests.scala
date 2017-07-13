package com.futurice.iodf

import com.futurice.iodf.df.{DocumentsTest, ObjectsTest, TableTest}
import com.futurice.iodf.perf.{BitsPerf, DfPerf, SeqPerf}
import com.futurice.testtoys.TestRunner

object Tests {
  def main(args: Array[String]): Unit = {
    TestRunner(
      "io/test",
      Seq(
        // core functionality tests
        new UtilsTest,
        new IoTypesTest,
        new SeqTest,
        new BitsTest,

        // df tests
        new ObjectsTest,
        new DocumentsTest,
        new TableTest,

        new MlTest,

        // perf tests
        new SeqPerf,
        new BitsPerf,
        new DfPerf
      ))
      .exec(args)
  }

}