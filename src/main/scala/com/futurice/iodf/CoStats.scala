package com.futurice.iodf

import com.futurice.iodf.ioseq.IoBits
import com.futurice.iodf.utils.LBits


case class CoStats(n:Long, fA:Long, fB:Long, fAB:Long, priorW:Double = 2, priorA:Double =0.5, priorB:Double =0.5) {

  def pA = MathUtils.eP(fA, n, priorA, priorW / priorA)
  def pB = MathUtils.eP(fB, n, priorB, priorW / priorB)

  def hA = MathUtils.h(pA)
  def hB = MathUtils.h(pB)

  val (naivePs, ps) = {
    val (naivePs, ps) = (new Array[Double](4), new Array[Double](4))
    (0 until 4).foreach { s =>
      val pAs = MathUtils.pS(MathUtils.relVarState(s, 0), pA)
      val pBs = MathUtils.pS(MathUtils.relVarState(s, 1), pB)
      val fS = MathUtils.relStateF(s, n, fA, fB, fAB)
      val naive = pAs * pBs
      naivePs(s) = naive
      ps(s) = MathUtils.eP(fS, n, naive, priorW / naive)
    }
    (naivePs, ps)
  }
  def d(relState:Int) : Double = {
    ps(relState) / naivePs(relState)
  }
  def d(as:Boolean, bs:Boolean) : Double = d(((if (as) 1 else 0)) + (if (bs) 2 else 0))
  def miPart(relState:Int) = {
    ps(relState) * MathUtils.log2(d(relState))
  }
  def mi = (0 until 4).map(miPart).sum
}

object CoStats {
  def apply(a: LBits, b: LBits, n:Long, priorW:Double, priorA:Double, priorB:Double) : CoStats = {
    CoStats(n, a.f, b.f, a.fAnd(b), priorW, priorA, priorB)
  }
  def apply(a: LBits, b: LBits, n:Long) : CoStats = {
    apply(a, b, n, 2.0, 0.5, 0.5)
  }
  def apply(a: LBits, b: LBits) : CoStats = {
    CoStats(a.lsize, a.f, b.f, a.fAnd(b))
  }
}