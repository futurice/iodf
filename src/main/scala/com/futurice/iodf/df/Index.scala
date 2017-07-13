package com.futurice.iodf.df

import java.io.Closeable

import com.futurice.iodf.{IoContext, IoScope}
import com.futurice.iodf._
import com.futurice.iodf.df.MultiDf.DefaultColIdMemRatio
import com.futurice.iodf.io._
import com.futurice.iodf.ml.CoStats
import com.futurice.iodf.providers.OrderingProvider
import com.futurice.iodf.store.{CfsDir, WrittenCfsDir}
import com.futurice.iodf.util.{LBits, LSeq, Ref}

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

object IndexConf {
  val emptySeq = Seq.empty[Any]
  val noAnalyzer = (v:Any) => emptySeq
}

case class IndexConf[ColId](analyzers:Map[ColId, Any => Seq[Any]] = Map[ColId, Any => Seq[Any]]()) {
  def analyze(field:ColId, value:Any) = {
    analyzer(field)(value)
  }
  def isAnalyzed(field:ColId) =
    (field != "this" && analyzers.get(field) != Some(IndexConf.noAnalyzer))
  def analyzer(field:ColId) : Any => Seq[Any] = {
    analyzers.getOrElse(field, v => Seq(v))
  }
  def withAnalyzer(field:ColId, analyzer: Any => Seq[Any]) = {
    new IndexConf(analyzers + (field -> analyzer))
  }
  def withoutField(field:ColId) = {
    withAnalyzer(field, IndexConf.noAnalyzer)
  }

}

trait IndexApi[ColId] {

  // Core API

  def colNameValues[T <: Any](colId:ColId) : LSeq[(ColId, T)]
  def colNameValuesWithIndex[T <: Any](colId:ColId) : Iterable[((ColId, T), Long)]
  def openIndex(idValue:(ColId, Any)) : LBits
  def openIndex(i:Long) : LBits

  // Helper functions

  def colValues[T  <: Any](colId:ColId) = {
    colNameValues[T](colId).map[T](_._2)
  }
  def index(idValue:(ColId, Any))(implicit scope:IoScope) : LBits = {
    scope.bind(openIndex(idValue))
  }
  def index(i:Long)(implicit scope:IoScope) : LBits = {
    scope.bind(openIndex(i))
  }

  def f(i:Long) = {
    using(openIndex(i)) { _.f }
  }
  def f(idValue:(ColId, Any)) = {
    using(openIndex(idValue)) { _.f }
  }

  // TODO: remove coStats
  def coStats(idValue1:(ColId, Any), idValue2:(ColId, Any)) = {
    using(openIndex(idValue1)) { b1 =>
      using (openIndex(idValue2)) { b2 =>
        CoStats(b1, b2)
      }
    }
  }
  def coStats(idValue1:Long, idValue2:Long) = {
    using(openIndex(idValue1)) { b1 =>
      using (openIndex(idValue2)) { b2 =>
        CoStats(b1, b2)
      }
    }
  }

}

object Index {

  def index(col:LSeq[_], analyzer:Any => Seq[Any], ordering:Ordering[Any]) : Array[(Any, LBits)] = {
    val distinct = col.toArray.flatMap(analyzer(_)).distinct.sorted(ordering)
    val rv = Array.fill(distinct.size)(new ArrayBuffer[Long]())
    val toIndex = distinct.zipWithIndex.toMap
    for (i <- (0L until col.lsize)) {
      analyzer(col(i)).foreach { token =>
        val idx = rv(toIndex(token))
        if (idx.size == 0 || idx.last != i) idx += i // avoid duplicates
      }
    }
    (distinct zip rv).map { case (value, trues) => (value, LBits.from(trues, col.lsize))}
  }

  def apply[ColId:Ordering](indexDf:Df[(ColId, Any)]) = {
    new Index[ColId](indexDf)
  }

  def from[ColId:Ordering](df:Df[ColId], conf:IndexConf[ColId]) : Index[ColId] = {
    val indexes = indexIterator(df, conf).toArray
    Index[ColId](
      Df[(ColId, Any)](
        LSeq.from(indexes.map(_._1)),
        LSeq.fill(indexes.length, typeOf[Boolean]),
        LSeq.from(indexes.map(_._2)),
        df.lsize)(indexColIdOrdering[ColId]))
  }

  def indexIterator[ColId](df:Df[ColId], conf:IndexConf[ColId]) = {
    val ordering = OrderingProvider.anyOrdering

    new Iterator[((ColId, Any), LBits)] {
      val colIds = df.colIds.iterator
      val colTypes = df.colTypes.iterator
      val cols = df._cols.iterator

      def getNexts : Option[(ColId, Iterator[(Any,LBits)])] = {
        colIds.hasNext match {
          case true =>
            val colId = colIds.next()
            val colType = colTypes.next
            using(cols.next) { col =>
              val analyzer = conf.analyzer(colId)

              index(col, analyzer, ordering).iterator match {
                case indexes if indexes.hasNext => Some((colId, indexes))
                case empty => getNexts
              }
            }
          case false =>
            None
        }
      }
      var nexts : Option[(ColId, Iterator[(Any,LBits)])] = getNexts

      override def hasNext: Boolean = {
        nexts match {
          case Some((_, nx)) => nx.hasNext
          case None => false
        }
      }
      override def next() : ((ColId, Any), LBits) = {
        val (colId, iter) = nexts.get
        val (value, bits) = iter.next
        if (nexts.get._2.isEmpty) nexts = getNexts
        (colId -> value, bits)
      }
    }
  }


  def indexColIdOrdering[ColId](implicit colOrd: Ordering[ColId]) = {
    new Ordering[(ColId, Any)] {
      val anyOrdering = OrderingProvider.anyOrdering

      override def compare(x: (ColId, Any), y: (ColId, Any)): Int = {
        colOrd.compare(x._1, y._1) match {
          case 0 => // fields matches, so the values should be of the same type
            anyOrdering.compare(x._2, y._2)
          case v => v
        }
      }
    }
  }


}

/**
  * Created by arau on 12.7.2017.
  */
class Index[ColId](val df:Df[(ColId, Any)],
                   closer : () => Unit = () => Unit)
  extends Df[(ColId, Any)] with IndexApi[ColId] {

  /**
    * All dataframes should be of form LBits :-/
    */
  type ColType[T] = LSeq[T]

  def view(from:Long, until:Long) : Index[ColId] =
    new Index[ColId](
      df.view(from, until))

  override def colTypes: LSeq[universe.Type] =
    new LSeq[Type] {
      override def apply(l: Long): universe.Type = typeOf[Boolean]
      override def lsize: Long = df.colCount
    }

  override def colIdOrdering: Ordering[(ColId, Any)] = df.colIdOrdering
  override def _cols: LSeq[_ <: LSeq[Any]] = df._cols

  override def colIds = df.colIds
  override def col[T <: Any](id:(ColId, Any))(implicit scope:IoScope) = df.col[T](id)
  override def col[T <: Any](i:Long)(implicit scope:IoScope) = df.col[T](i)

  def colNameValues[T <: Any](colId:ColId) : LSeq[(ColId, T)] = {
    val from =
      df.indexFloorAndCeil(colId -> MinBound())._3
    val until =
      df.indexFloorAndCeil(colId -> MaxBound())._3
    df.colIds.view(from, until).map[(ColId, T)] { case (key, value) => (key, value.asInstanceOf[T]) }
  }
  def colNameValuesWithIndex[T <: Any](colId:ColId) : Iterable[((ColId, T), Long)] = {
    val from =
      df.indexFloorAndCeil(colId -> MinBound())._3
    val until =
      df.indexFloorAndCeil(colId -> MaxBound())._3
    df.colIds.view(from, until).zipWithIndex.map {
      case ((key, value), index) =>
        ((key, value.asInstanceOf[T]), index + from)
    }
  }

  def openIndex(idValue:(ColId, Any)) : LBits = {
    df.indexOf(idValue) match {
      case -1 => LBits.empty(df.lsize)
      case i  => openIndex(i)
    }
  }
  def openIndex(i:Long) : LBits = {
    df.openCol(i).asInstanceOf[LBits]
  }

  def lsize = df.lsize

  def n = df.lsize

  override def close(): Unit = {
    df.close
    closer()
  }

}

class IndexIoType[ColId:TypeTag:Ordering](val dfType:DfIoType[(ColId, Any)])
  extends MergeableIoType[Index[ColId], Index[ColId]] {

  override def interfaceType: universe.Type = typeOf[Index[ColId]]

  override def ioInstanceType: universe.Type = typeOf[Index[ColId]]

  override def viewMerged(seqs: Seq[Ref[Index[ColId]]]): Index[ColId] =
    Index[ColId](dfType.viewMerged(seqs.map(_.map(_.df))))

  override def open(ref: DataAccess): Index[ColId] =
    Index[ColId](dfType.open(ref))

  override def write(out: DataOutput, iface: Index[ColId]): Unit =
    dfType.write(out, iface.df)
}

