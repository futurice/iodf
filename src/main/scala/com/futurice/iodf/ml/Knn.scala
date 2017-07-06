package com.futurice.iodf.ml

import java.io.Closeable

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import com.futurice.iodf._
import com.futurice.iodf.ioseq.{DenseIoBits, IoBits}
import com.futurice.iodf.Utils._
import com.futurice.iodf.df.{IndexConf, IndexedDf, TypedDf}
import com.futurice.iodf.util.LBits


/**
  * NOTE; when scoring knn(X), and examining some entry E from given dataframe:
  *   the first key weight in keyValueW is the distance, when for feature
  *   f(E) is true, but f(X) is false, and w._2 is the distance, when
  *   f(E) is false, buf f(X) is true
  *
  * Created by arau on 22.3.2017.
  */
class Knn[T](df:IndexedDf[T],
             select:LBits,
             indexConf:IndexConf[String],
             keyValueW:Map[(String, Any), (Double, Double)])(implicit tag:ClassTag[T],tt:TypeTag[T], ioContext:IoContext) {

  val schema = TypedDf.typeSchema[T]

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

  def distances(v:T) = {
    val rv = new Array[Double](baseDistance.length)
    var baseLine = 0.0
    df.colIds.foreach { id =>
      schema.getter(id).foreach { getter =>
        indexConf.analyze(id, getter(v)).foreach { value =>
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
    }
    select.trues.map(_.toInt).foreach { i =>
      rv(i) += baseDistance(i) + baseLine
    }
    rv
  }

  def knn(k:Int, v:T, filter:Option[Long => Boolean] = None) = {
    val ds = distances(v)
    val sorted =
      select.trues.map(i => (ds(i.toInt), i)).toArray.sortBy(_._1)
    filter.map(f => sorted.filter(e => f(e._2))).getOrElse(sorted).take(k)
  }

}

object Knn {

  def keyValueWeights[T]( df:IndexedDf[T],
                          in:Set[String],
                          outTrues:LBits,
                          outDefined:LBits,
                          varDFilter:Double)(implicit ioContext:IoContext)
    : Map[(String, Any), (Double, Double)] =
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

  def keyValueWeights[T]( df:IndexedDf[T],
                          in:Set[String],
                          predicted:(String,Any),
                          varDFilter:Double)(
                          implicit scope:IoScope,
                          io:IoContext) : Map[(String, Any), (Double, Double)] =
    keyValueWeights(
      df,
      in,
      df.index(predicted),
      scope.bind(io.bits.create(
        LBits((0 until df.size).map(i => true)))), // true vector
      varDFilter)

  def apply[T](df:IndexedDf[T],
               indexConf:IndexConf[String],
               in:Set[String],
               predicted:(String, Any),
               varDFilter:Double)(
               implicit tag:ClassTag[T],
               tt:TypeTag[T],
               scope:IoScope,
               io:IoContext) = {
    new Knn(df,
            LBits((0L until df.lsize).map(e => true)),
            indexConf,
            keyValueWeights(
              df,
              in,
              predicted,
              varDFilter))
  }
}