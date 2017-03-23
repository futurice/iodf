package com.futurice.iodf.ml

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

import com.futurice.iodf._
import com.futurice.iodf.ioseq.IoBits
import com.futurice.iodf.Utils._


/**
  * Created by arau on 22.3.2017.
  */
class Knn[IoId, T]( df:IndexedDf[IoId, T],
                    indexConf:IndexConf[String],
                    keyValueW:Map[(String, Any), Double])(implicit tag:ClassTag[T],tt:TypeTag[T]) {

  val schema = Dfs.default.typeSchema[T]

  val baseDistance = {
    val rv = new Array[Double](df.size)
    keyValueW.foreach { case (keyValue, w) =>
      val index = df.indexDf.indexOf(keyValue)
      if (index >= 0 && index < df.indexDf.colIds.size) {
        using(df.openIndex(index)) { bits =>
          bits.trues.foreach { t =>
            rv(t.toInt) += w
          }
        }
      }
    }
    rv
  }

  def distances(v:T) = {
    val rv = baseDistance.clone()
    df.colIds.foreach { id =>
      schema.getter(id).foreach { getter =>
        indexConf.analyze(id, getter(v)).foreach { value =>
          val keyValue = id -> value
          keyValueW.get(keyValue).foreach { case w =>
            using(df.openIndex(keyValue)) { bits =>
              bits.trues.foreach { t =>
                rv(t.toInt) -= w //
              }
            }
          }
        }
      }
    }
    rv
  }

  def knn(k:Int, v:T) = {
    distances(v).zipWithIndex.sortBy(_._1).take(k)
  }

}

object Knn {

  def keyValueWeights[IoId, T]( df:IndexedDf[IoId, T],
                                in:Set[String],
                                outTrues:IoBits[_],
                                outDefined:IoBits[_],
                                varDFilter:Double) : Map[(String, Any), Double] =
    df.indexDf.colIds.zipWithIndex.filter(e => in.contains(e._1._1)).map { case (keyValue, index) =>
      using (df.openIndex(index)) { case bits =>
        scoped { implicit scope =>
          implicit val io = IoContext()
          val stats = CoStats(bits & outDefined, outTrues, outDefined.f)
          (keyValue, Math.abs(Math.log(stats.d(true, true) / stats.d(false, true))))
        }
      }
    }.filter(_._2 >= varDFilter).toMap

  def keyValueWeights[IoId, T]( df:IndexedDf[IoId, T],
                                in:Set[String],
                                predicted:(String,Any),
                                varDFilter:Double)(
                                implicit scope:IoScope,
                                io:IoContext[Int]) : Map[(String, Any), Double] =
    keyValueWeights(
      df,
      in,
      df.index(predicted),
      scope.bind(io.bits.create(
        io.dir,
        (0 until df.size).map(i => true))), // true vector
      varDFilter)

  def apply[IoId, T](df:IndexedDf[IoId, T],
                  indexConf:IndexConf[String],
                  in:Set[String],
                  predicted:(String, Any),
                  varDFilter:Double)(implicit tag:ClassTag[T], tt:TypeTag[T], scope:IoScope, io:IoContext[Int]) = {
    new Knn(df,
            indexConf,
            keyValueWeights(
              df,
              in,
              predicted,
              varDFilter))
  }
}