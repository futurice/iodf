package com.futurice.iodf.df

import java.io.Closeable

import com.futurice.iodf.{IoContext, IoScope}
import com.futurice.iodf._
import com.futurice.iodf.df.MultiCols.DefaultColIdMemRatio
import com.futurice.iodf.io._
import com.futurice.iodf.ml.CoStats
import com.futurice.iodf.providers.OrderingProvider
import com.futurice.iodf.store.{CfsDir, WrittenCfsDir}
import com.futurice.iodf.util._

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

  def colIdValues[T <: Any](colId:ColId) : LSeq[(ColId, T)]
  def colIdValuesWithIndex[T <: Any](colId:ColId) : Iterable[((ColId, T), Long)]
  def openIndex(idValue:(ColId, Any)) : LBits
  def openIndex(i:Long) : LBits

  // Helper functions

  def colValues[T  <: Any](colId:ColId) = {
    colIdValues[T](colId).map[T](_._2)
  }
  def index(idValue:(ColId, Any))(implicit scope:IoScope) : LBits = {
    scope.bind(openIndex(idValue))
  }
  def index(i:Long)(implicit scope:IoScope) : LBits = {
    scope.bind(openIndex(i))
  }

  def openedIndexes : LSeq[LBits]

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

  def empty[ColId](lsize:Long)(implicit ord: Ordering[(ColId, Any)]) : Index[ColId] = {
    new Index[ColId](
      Ref.mock(Cols.empty[(ColId, Any)](lsize)),
      () => Unit)
  }

  def index(col:LSeq[_], analyzer:Any => Seq[Any], ordering:Ordering[Any]) : Array[(Any, LBits)] = {
    val distinct =
      col.zipWithIndex.toArray.flatMap( e =>
        try {
          analyzer(e._1)
        } catch {
          case ex : Exception =>
            throw new IllegalArgumentException("processing row " + e._2 + " value " + e._1 + " failed.", ex)
        }
      ).distinct.sorted(ordering)
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

  def apply[ColId:Ordering](indexDf:Cols[(ColId, Any)],
                            closer:Closeable = Utils.dummyCloseable) = scoped { implicit bind =>
    new Index[ColId](Ref(indexDf), closer)
  }

  def from[ColId:Ordering](df:Cols[ColId], conf:IndexConf[ColId]) : Index[ColId] = {
    val indexes = indexIterator(df, conf).toArray
    Index[ColId](
      Cols[(ColId, Any)](
        ColSchema(
          LSeq.from(indexes.map(_._1)),
          LSeq.fill(indexes.length, typeOf[Boolean]),
          LSeq.fill(indexes.length, KeyMap.empty))(indexColIdOrdering[ColId]),
        LSeq.from(indexes.map(_._2)),
        df.lsize))
  }

  def indexIterator[ColId](df:Cols[ColId], conf:IndexConf[ColId]) = {
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

              (try {
                index(col, analyzer, ordering).iterator
              } catch {
                case e: Exception =>
                  throw new IllegalArgumentException("indexing column " + colId + " failed.", e)
              }) match {
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
class Index[ColId](_dfRef:Ref[Cols[(ColId, Any)]],
                   closer : Closeable = Utils.dummyCloseable)
  extends Cols[(ColId, Any)] with IndexApi[ColId] with ColsWrap[(ColId, Any), Cols[(ColId, Any)]] {

  val dfRef = _dfRef.openCopy

  val df = dfRef.get

  def wrappedCols = df

  Tracing.opened(this)

  /**
    * All dataframes should be of form LBits :-/
    */
  type ColType[T] = LSeq[T]

  override def openView(from:Long, until:Long) : Index[ColId] = scoped { implicit bind =>
    new Index[ColId](dfRef.copyMap(_.openView(from, until)))
  }

  val schema = ColSchema( // optimize type lookups
    df.schema.colIds,
    new LSeq[Type] {
      val booleanType = typeOf[Boolean]
      override def apply(l: Long): universe.Type = booleanType
      override def lsize: Long = df.colCount
    },
    df.colMetas)(df.colIdOrdering)

  override def _cols: LSeq[_ <: LSeq[Any]] = df._cols
  def openedIndexes : LSeq[LBits] =
    _cols.lazyMap { _.asInstanceOf[LBits] }


  override def colIds = df.colIds
  override def col[T <: Any](id:(ColId, Any))(implicit scope:IoScope) = df.col[T](id)
  override def col[T <: Any](i:Long)(implicit scope:IoScope) = df.col[T](i)


  def colIdValues[T <: Any](colId:ColId) : LSeq[(ColId, T)] = {
    val from =
      df.indexFloorAndCeil(colId -> MinBound())._3
    val until =
      df.indexFloorAndCeil(colId -> MaxBound())._3
    df.colIds.view(from, until).map[(ColId, T)] { case (key, value) => (key, value.asInstanceOf[T]) }
  }
  def colIdValuesWithIndex[T <: Any](colId:ColId) : LSeq[((ColId, T), Long)] = {
    val from =
      df.indexFloorAndCeil(colId -> MinBound())._3
    val until =
      df.indexFloorAndCeil(colId -> MaxBound())._3
    df.colIds.view(from, until).zipWithIndex.lazyMap {
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
    Tracing.closed(this)
    dfRef.close
    closer.close()
  }

  override def openSelect(indexes:LSeq[Long]) = scoped { implicit bind =>
    new Index(dfRef.copyMap(_.openSelect(indexes)))
  }
  override def openSelectCols(indexes:LSeq[Long]) = scoped { implicit bind =>
    new Index(dfRef.copyMap(_.openSelectCols(indexes)))
  }

  override def openSelectSome(indexes:LSeq[Option[Long]]) = scoped { implicit bind =>
    val selection = Selection(indexes)
    new Index(
      dfRef.copyMap[Cols[(ColId, Any)]]( df =>
        Cols[(ColId, Any)](
          schema,
          _cols.lazyMap { c =>
            c.asInstanceOf[LBits].selectSomeStates(selection).bind(c)
          },
          indexes.lsize)))
  }

  def selectSome(indexes:LSeq[Option[Long]])(implicit bind:IoScope) = {
    bind(openSelectSome(indexes))
  }

  /* TODO: should this be renamed to mapIndexKeys */
  def openMapIndexKeys[ColId2](f : ColId => ColId2)(implicit ord2:Ordering[(ColId2, Any)]) = scoped { implicit bind =>
    new Index(
      dfRef.copyMap[Cols[(ColId2, Any)]] { df =>
        Cols[(ColId2, Any)](
          schema.openMapColIds { case (id, value) => f(id) -> value },
          _cols,
          lsize)
      })
  }

  def mapColIds[ColId2](f : ColId => ColId2)(implicit ord:Ordering[(ColId2, Any)], bind:IoScope) = {
    bind(openMapIndexKeys[ColId2](f)(ord))
  }

  /**
    * Note, this can be memory heavy operation
    */
  def toBitRows = {
    val rows = Array.fill(lsize.toInt)(new ArrayBuffer[Long]())
    openedIndexes.zipWithIndex.foreach { case (openedBits, featureIndex) =>
      using(openedBits) { bits =>
        bits.trues.foreach { i =>
          rows(i.toInt) += featureIndex
        }
      }
    }
    val cc = colCount
    LSeq.from(rows.map(r => LBits.from(r, cc)))
  }

}

class IndexIoType[ColId:TypeTag:Ordering](val dfType:ColsIoType[(ColId, Any)])
  extends MergeableIoType[Index[ColId], Index[ColId]] {

  override def interfaceType: universe.Type = typeOf[Index[ColId]]

  override def ioInstanceType: universe.Type = typeOf[Index[ColId]]

  override def openViewMerged(seqs: Seq[Ref[Index[ColId]]]): Index[ColId] =
    Index[ColId](dfType.openViewMerged(seqs.map(_.map(_.df))))

  override def open(ref: DataAccess): Index[ColId] =
    Index[ColId](dfType.open(ref))

  override def write(out: DataOutput, iface: Index[ColId]): Unit =
    dfType.write(out, iface.df)
}

