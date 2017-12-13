package com.futurice.iodf.ml

object MathUtils {
  val INV_LOG2 = 1/Math.log(2)
  def log2(v:Double) = Math.log(v) * INV_LOG2

  /** https://en.wikipedia.org/wiki/Inverse-variance_weighting */
  def inverseVarianceWeighting(est1:Double, var1:Double, est2:Double, var2:Double) = {
    if (var1 == 0.0) est1
    else if (var2 == 0.0) est2
    else
      (est1 / var1 + est2 / var2) / (1 / var1 + 1 / var2)
  }

  def eP2(f:Long, n:Long, priorP:Double, priorW:Double = 1.0, biasPriorW:Double = 4.0) = {
    val priorVar = priorP * (1-priorP)
    if (n == 0 || priorVar == 0.0) priorP
    else {
      val freqEst = f / n.toDouble
      val bias = priorP - eP(f, n, priorP, biasPriorW / priorVar)

      inverseVarianceWeighting(
        freqEst,
        priorW/n,
        priorP,
        priorVar +bias*bias)
    }
  }

  def eP(f:Long, n:Long, priorP:Double, priorW:Double) = {
    (f + priorP * priorW) / (n + priorW).toDouble
  }
  def l(p:Double) = -log2(p)
  def h(p:Double) = {
    p * -log2(p) + (1-p) * -log2(1-p)
  }
  def pS(s:Boolean, p:Double) = {
    if (s) p else (1-p)
  }
  def relStateVarState(relState:Int, v:Int) = {
    (relState & (1 << v)) > 0
  }
  def relStateF(relState:Int, n:Long, fA:Long, fB:Long, fAB:Long) = {
    relState match {
      case 0 => n - fA - fB + fAB
      case 1 => fA - fAB  // a is true
      case 2 => fB - fAB  // b is true
      case 3 => fAB       // a&b are true
    }
  }

  def varPsToNaiveRelStatePs(varPs:Array[Double]) = {
    val relStateCount = 1 << varPs.size
    (0 until relStateCount ).map { s =>
      (0 until varPs.size).map { i =>
        MathUtils.pS(MathUtils.relStateVarState(s, i), varPs(i))
      }.product
    }.toArray
  }
}


