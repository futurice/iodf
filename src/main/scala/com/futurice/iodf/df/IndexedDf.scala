package com.futurice.iodf.df

import java.io.Closeable

import com.futurice.iodf.Utils.{scoped, _}
import com.futurice.iodf._
import com.futurice.iodf.df.MultiDf.DefaultColIdMemRatio
import com.futurice.iodf.io._
import com.futurice.iodf.ioseq.{IoSeq, SeqIoType}
import com.futurice.iodf.ml.CoStats
import com.futurice.iodf.providers.OrderingProvider
import com.futurice.iodf.store.{CfsDir, WrittenCfsDir}
import com.futurice.iodf.util.{LBits, LSeq, PeekIterator, Ref}

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

object IndexedDf {

  def viewMerged[T:TypeTag:ClassTag](dfs:Seq[IndexedDf[T]], colIdMemRatio:Int = DefaultColIdMemRatio)(
    implicit io:IoContext) : IndexedDf[T]= {
    IndexedDf[T](
      TypedDf.viewMerged[T](dfs.map(_.df), colIdMemRatio),
      MultiDf[(String, Any)](dfs.map(_.indexDf), IndexedDf.indexMerging)(indexColIdOrdering))
  }


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
    (distinct zip rv).map { case (value, trues) => (value, LBits(trues, col.lsize))}
  }

  def index[ColId:Ordering](df:Df[ColId], conf:IndexConf[ColId]) : Df[(ColId, Any)]= {
    val indexes = indexIterator(df, conf).toArray
    Df[(ColId, Any)](
       LSeq(indexes.map(_._1)),
       LSeq.fill(indexes.length, typeOf[Boolean]),
       LSeq(indexes.map(_._2)),
       df.lsize)(indexColIdOrdering[ColId])
  }
  def apply[T:ClassTag](df:TypedDf[T], indexDf:Df[(String, Any)]) : IndexedDf[T] = {
    new IndexedDf[T](df, indexDf)
  }
  def apply[T:ClassTag:TypeTag](df:TypedDf[T], conf:IndexConf[String]) : IndexedDf[T] = {
    IndexedDf[T](df, index(df, conf))
  }
  def apply[T:ClassTag:TypeTag](data:Seq[T], conf:IndexConf[String]) : IndexedDf[T] = {
    val df = TypedDf[T](data)
    IndexedDf[T](df, conf)
  }
  def indexIterator[ColId](df:Df[ColId], conf:IndexConf[ColId]) = {
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
              val ordering = OrderingProvider.orderingOf(colType)

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

  def indexMerging(implicit io:IoContext) = {
    new DfMerging[(String, Any)] {
      def colMerging(col: (String, Any)) =
        io.bits
      def colMerging(index:Long) =
        io.bits
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

class IndexedDf[T](val df:TypedDf[T],
                   val indexDf:Df[(String, Any)],
                   closer : () => Unit = () => Unit) extends Closeable {

  def view(from:Long, until:Long) : IndexedDf[T] =
    new IndexedDf[T](
      df.view(from, until),
      indexDf.view(from, until))

  def apply(i:Long) = df(i)
  def colIds = df.colIds
  def col[T <: Any](id:String)(implicit scope:IoScope) = df.col[T](id)
  def col[T <: Any](i:Long)(implicit scope:IoScope) = df.col[T](i)

  def colNameValues[T <: Any](colId:String) : LSeq[(String, T)] = {
    val from =
      indexDf.indexFloorAndCeil(colId -> MinBound())._3
    val until =
      indexDf.indexFloorAndCeil(colId -> MaxBound())._3
    indexDf.colIds.view(from, until).map[(String, T)] { case (key, value) => (key, value.asInstanceOf[T]) }
  }
  def colNameValuesWithIndex[T <: Any](colId:String) : Iterable[((String, T), Long)] = {
    val from =
      indexDf.indexFloorAndCeil(colId -> MinBound())._3
    val until =
      indexDf.indexFloorAndCeil(colId -> MaxBound())._3
    indexDf.colIds.view(from, until).zipWithIndex.map {
      case ((key, value), index) =>
        ((key, value.asInstanceOf[T]), index + from)
    }
  }
  def colValues[T  <: Any](colId:String) = {
    colNameValues[T](colId).map[T](_._2)
  }

  def index(idValue:(String, Any))(implicit scope:IoScope) : LBits = {
    scope.bind(openIndex(idValue))
  }
  def index(i:Long)(implicit scope:IoScope) : LBits = {
    scope.bind(openIndex(i))
  }
  def openIndex(idValue:(String, Any)) : LBits = {
    indexDf.indexOf(idValue) match {
      case -1 => LBits.empty(indexDf.lsize)
      case i  => openIndex(i)
    }
  }
  def openIndex(i:Long) : LBits = {
    indexDf.openCol(i).asInstanceOf[LBits]
  }

  def lsize = df.lsize
  def size = lsize.toInt

  def n = df.lsize
  def f(i:Long) = {
    using(openIndex(i)) { _.f }
  }
  def f(idValue:(String, Any)) = {
    using(openIndex(idValue)) { _.f }
  }
  def coStats(idValue1:(String, Any), idValue2:(String, Any)) = {
    using(openIndex(idValue1)) { b1 =>
      using (openIndex(idValue2)) { b2 =>
        CoStats(b1, b2)
      }
    }
  }
  def coStats(idValue1:Int, idValue2:Int) = {
    using(openIndex(idValue1)) { b1 =>
      using (openIndex(idValue2)) { b2 =>
        CoStats(df.lsize, b1.f, b2.f, b1.fAnd(b2))
      }
    }
  }

  override def close(): Unit = {
    df.close
    indexDf.close
  }

  def as[E : ClassTag](implicit tag:TypeTag[E]) : IndexedDf[E] = {
    new IndexedDf[E](df.as[E], new DfRef(indexDf))
  }
}


class IndexedDfIoType[T:ClassTag:TypeTag](
    dfType:TypedDfIoType[T],
    indexType:DfIoType[(String, Any)])(implicit io:IoContext) extends MergeableIoType[IndexedDf[T], IndexedDf[T]] {

  // Ints are fast and compact
  type CfsFileId = Int
  def DfFileId = 0
  def IndexDfId = 1

  implicit val types = indexType.types

  override def interfaceType: universe.Type = typeOf[IndexedDf[T]]
  override def ioInstanceType: universe.Type = typeOf[IndexedDf[T]]

  override def open(data: DataAccess): IndexedDf[T] = scoped { implicit bind =>
    val dirRef = bind(Ref.open(CfsDir.open[CfsFileId](data)))
    val dir = dirRef.get
    val df = dfType.open(dir.access(DfFileId))
    val indexDf = indexType.open(dir.access(IndexDfId))
    new IndexedDf[T](df, indexDf, () => dirRef.close)
  }

  override def write(out: DataOutput, df: IndexedDf[T]): Unit = scoped { implicit bind =>
    val dir = bind(WrittenCfsDir.open[CfsFileId](out))
    dfType.save(dir.ref(DfFileId), df.df)
    indexType.save(dir.ref(IndexDfId), df.indexDf)
  }

  def writeIndexed(out:DataOutput, df:TypedDf[T], indexConf:IndexConf[String]) : Unit = scoped { implicit bind =>
    val dir = bind(WrittenCfsDir.open[CfsFileId](out))
    dfType.write(out, df)
    indexType.writeAsDf(
      out,
      df.lsize,
      IndexedDf.indexIterator[String](df, indexConf).map { case (colId, bits) =>
        (colId, typeOf[Boolean]) -> bits
      })
  }

  override def viewMerged(seqs: Seq[IndexedDf[T]]): IndexedDf[T] = {
    IndexedDf(
      dfType.viewMerged(seqs.map(_.df)),
      MultiDf[(String, Any)](seqs.map(_.indexDf), IndexedDf.indexMerging)(
        IndexedDf.indexColIdOrdering[String]
      )
    )
  }
}