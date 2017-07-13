package com.futurice.iodf

import com.futurice.iodf.util.{LBits, LSeq}
import com.futurice.testtoys.TestSuite

import scala.reflect.runtime.universe._

/**
  * Created by arau on 13.7.2017.
  */
class IoTypesTest extends TestSuite("io-types") {

  test("type-comparisons") { t =>

    def tCompare(a:Type, b:Type) = {
      t.tln(f"($a <:< $b) == ${a <:< b}")
      t.tln(f"($b <:< $a) == ${b <:< a}")
      t.tln
    }

    tCompare(typeOf[(String, Any)], typeOf[(String, Int)])
    tCompare(typeOf[LSeq[(String, Any)]], typeOf[LSeq[(String, Int)]])

  }

  test("io-type-of") { t =>
    scoped { implicit bind =>

      val io = IoContext()

      def tIoTypeOf[T:TypeTag] = {
        val typ = typeOf[T]
        t.tln(f"$typ -> ${io.types.ioTypeOf(typ)}")
      }

      tIoTypeOf[LSeq[(String,Int)]]
      tIoTypeOf[LSeq[(String,Any)]]
      tIoTypeOf[LSeq[Boolean]]
      tIoTypeOf[LBits]



    }
  }

}
