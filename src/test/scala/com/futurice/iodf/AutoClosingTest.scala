package com.futurice.iodf

import com.futurice.iodf.util.AutoClosing
import com.futurice.testtoys.{TestSuite, TestTool}

case class TestCloseable(name:String, t:TestTool) extends AutoCloseable {
  t.tln(f"  - $name opened")
  var closed = false
  def close = {
    t.tln(f"  - $name closed")
    closed = true
  }
  override def toString = name

}
class AutoClosingTest extends TestSuite("autoClosing") {

  def bLife(ac:AutoClosing, t:TestTool): Unit = {
    t.tln("b on the other hand has a limited scope...")
    val b = ac.add(TestCloseable("b", t))
  }

  test("basics") { t =>
    using (AutoClosing()) { ac =>

      {
        t.tln("a will be here for a while")
        val a = ac.add(TestCloseable("a", t))

        bLife(ac, t)

        t.tln("b became unreachable, let's collect the garbage")
        System.gc()
        ac.clean
      }
      t.tln("now a is unreachable, let's collect the garbage")
      System.gc()
      ac.clean()
    }
  }

}
