package com.futurice.iodf

import com.futurice.testtoys.TestRunner

object Tests {
  def main(args:Array[String]): Unit = {
    TestRunner(
      "io/test",
      Seq(
        new DfTest))
      .exec(args)
  }
}
