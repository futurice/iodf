package com.futurice.iodf.ml

import java.io.Closeable

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import com.futurice.iodf._
import com.futurice.iodf.ioseq.{DenseIoBits, IoBits}
import com.futurice.iodf.Utils._


/**
  * Created by arau on 22.3.2017.
  */
class Knn[IoId, T](df:IndexedDf[IoId, T],
                   select:IoBits[_],
                   indexConf:IndexConf[String],
                   keyValueW:Map[(String, Any), (Double, Double)])(implicit tag:ClassTag[T],tt:TypeTag[T]) {

  val schema = Dfs.fs.typeSchema[T]

  val baseDistance = {
    using (IoContext.open) { implicit io =>
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
  }

  def distances(v:T) = {
    using (IoContext.open) { implicit io =>
      val rv = new Array[Double](baseDistance.length)
      var baseLine = 0.0
      df.colIds.foreach { id =>
        schema.getter(id).foreach { getter =>
          indexConf.analyze(id, getter(v)).foreach { value =>
            val keyValue = id -> value
            keyValueW.get(keyValue).foreach { case w =>
              baseLine += w._1
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
  }

  def knn(k:Int, v:T, filter:Option[Long => Boolean] = None) = {
    val ds = distances(v)
    val sorted =
      select.trues.map(i => (ds(i.toInt), i)).toArray.sortBy(_._1)
    filter.map(f => sorted.filter(e => f(e._2))).getOrElse(sorted).take(k)
  }

}

object Knn {

  def keyValueWeights[IoId, T]( df:IndexedDf[IoId, T],
                                in:Set[String],
                                outTrues:IoBits[_],
                                outDefined:IoBits[_],
                                varDFilter:Double) : Map[(String, Any), (Double, Double)] =
    df.indexDf.colIds.zipWithIndex.filter(e => in.contains(e._1._1)).map { case (keyValue, index) =>
      using (df.openIndex(index)) { case bits =>
        scoped { implicit scope =>
          implicit val io = IoContext()
          val stats = CoStats(bits & outDefined, outTrues, outDefined.f)
          (keyValue,
            (Math.abs(Math.log(stats.d(false, true) / stats.d(false, false))),
             Math.abs(Math.log(stats.d(true, true) / stats.d(true, false)))))
        }
      }
    }.filter(_._2._1 >= varDFilter).toMap

  def keyValueWeights[IoId, T]( df:IndexedDf[IoId, T],
                                in:Set[String],
                                predicted:(String,Any),
                                varDFilter:Double)(
                                implicit scope:IoScope,
                                io:IoContext[Int]) : Map[(String, Any), (Double, Double)] =
    keyValueWeights(
      df,
      in,
      df.index(predicted),
      scope.bind(io.bits.createDense(
        io.dir,
        (0 until df.size).map(i => true))), // true vector
      varDFilter)

  def apply[IoId, T](df:IndexedDf[IoId, T],
                  indexConf:IndexConf[String],
                  in:Set[String],
                  predicted:(String, Any),
                  varDFilter:Double)(implicit tag:ClassTag[T], tt:TypeTag[T], scope:IoScope, io:IoContext[Int]) = {
    new Knn(df,
            DenseIoBits((0L until df.lsize).map(e => true)),
            indexConf,
            keyValueWeights(
              df,
              in,
              predicted,
              varDFilter))
  }
}