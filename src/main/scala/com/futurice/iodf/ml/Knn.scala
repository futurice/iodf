

package com.futurice.iodf.ml

import java.io.Closeable

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import com.futurice.iodf._
import com.futurice.iodf.ioseq.{DenseIoBits, IoBits}
import com.futurice.iodf._
import com.futurice.iodf.df._
import com.futurice.iodf.util.LBits


/**
  * NOTE; when scoring knn(X), and examining some entry E from given dataframe:
  *   the first key weight in keyValueW is the distance, when for feature
  *   f(E) is true, but f(X) is false, and w._2 is the distance, when
  *   f(E) is false, buf f(X) is true
  *
  * Created by arau on 22.3.2017.
  */
class Knn[ColId](df:Indexed[ColId, _ <: Cols[ColId]],
                 select:LBits,
                 indexConf:IndexConf[ColId],
                 keyValueW:Map[(ColId, Any), (Double, Double)])(implicit ioContext:IoContext) {

  val baseDistance = {
    val rv = new Array[Double](df.size)
    keyValueW.foreach { case (keyValue, w) =>
      val index = df.indexDf.indexOf(keyValue)
      if (index >= 0 && index < df.indexDf.colIds.size) {
        scoped { implicit scope =>
          (df.index(index) & select).trues.foreach { t =>
            rv(t.toInt) += w._1
          }
        }
      }
    }
    rv
  }

  def distances(row:Array[Any]) = {
    val rv = new Array[Double](baseDistance.length)
    var baseLine = 0.0
    (df.colIds zip row).foreach { case (id, v) =>
      indexConf.analyze(id, v).foreach { value =>
        val keyValue = id -> value
        keyValueW.get(keyValue).foreach { case w =>
          baseLine += w._2
          scoped { implicit scope =>
            (df.index(keyValue) & select).trues.foreach { t =>
              rv(t.toInt) -= (w._1 + w._2) //
            }
          }
        }
      }
    }
    select.trues.map(_.toInt).foreach { i =>
      rv(i) += baseDistance(i) + baseLine
    }
    rv
  }

  def knn(k:Int, row:Array[Any], filter:Option[Long => Boolean] = None) = {
    val ds = distances(row)
    val sorted =
      select.trues.map(i => (ds(i.toInt), i)).toArray.sortBy(_._1)
    filter.map(f => sorted.filter(e => f(e._2))).getOrElse(sorted).take(k)
  }

}

object Knn {

  def keyValueWeights[ColId](df:Indexed[ColId, _ <: Cols[ColId]],
                             in:Set[ColId],
                             outTrues:LBits,
                             outDefined:LBits,
                             varDFilter:Double)(implicit ioContext:IoContext)
    : Map[(ColId, Any), (Double, Double)] =
    df.indexDf.colIds.zipWithIndex.filter(e => in.contains(e._1._1)).map { case (keyValue, index) =>
      using (df.openIndex(index)) { case bits =>
        scoped { implicit scope =>
          val stats = CoStats(bits & outDefined, outTrues, outDefined.f)
          (keyValue,
            (Math.abs(Math.log(stats.d(false, true) / stats.d(false, false))),
             Math.abs(Math.log(stats.d(true, true) / stats.d(true, false)))))
        }
      }
    }.filter(_._2._1 >= varDFilter).toMap

  def keyValueWeights[ColId](df:Indexed[ColId, _ <: Cols[ColId]],
                             in:Set[ColId],
                             predicted:(ColId,Any),
                             varDFilter:Double)(
                             implicit scope:IoScope,
                             io:IoContext) : Map[(ColId, Any), (Double, Double)] =
    keyValueWeights[ColId](
      df,
      in,
      df.index(predicted),
      scope.bind(io.bits.create(
        LBits.from((0 until df.size).map(i => true)))), // true vector
      varDFilter)

  def apply[ColId](df:Indexed[ColId, _ <: Cols[ColId]],
                   indexConf:IndexConf[ColId],
                   in:Set[ColId],
                   predicted:(ColId, Any),
                   varDFilter:Double)(
                   implicit scope:IoScope,
                   io:IoContext) = {
    new Knn[ColId](df,
                   LBits.from((0L until df.lsize).map(e => true)),
                   indexConf,
                   keyValueWeights(
                     df,
                     in,
                     predicted,
                     varDFilter))
  }
}