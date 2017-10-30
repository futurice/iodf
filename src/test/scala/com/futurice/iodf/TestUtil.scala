package com.futurice.iodf

import com.futurice.iodf.df.Cols
import com.futurice.iodf.util.{LSeq, Tracing}
import com.futurice.testtoys.TestTool

/**
  * Created by arau on 13.7.2017.
  */
object TestUtil {

  def tRefCount(t: TestTool) = {
    t.tln(Tracing.openItems + " refs open.")
  }
  def tSeq[T](t:TestTool, seq:LSeq[T]) = {
    t.tln("contents:")
    t.tln
    seq.foreach { s =>
      t.tln("  " + s)
    }
    t.tln
  }

  def tDf[ColId](t:TestTool, df:Cols[ColId]) = {
    t.tln
    t.tln("colIds are: ")
    t.tln
    df.colIds.zipWithIndex.foreach {
      case (id, index) =>
        t.tln("  " + id + " " + df.colMetas(index).map(kv => kv.key.name + ":" + kv.value).mkString(", "))
    }
    t.tln
    t.tln("columns are: ")
    t.tln
    (0L until df.colCount).map { i =>
      using(df.openCol[Any](i)) { col =>
        t.tln("  " + col.map { b: Any =>
          b.toString
        }.mkString(","))
      }
    }
    t.tln
  }

}
