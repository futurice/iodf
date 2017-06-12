package com.futurice.iodf

import java.io.Closeable

import com.futurice.iodf.Utils.using
import com.futurice.iodf.ioseq.{IoBits}
import com.futurice.iodf.utils.LBits


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

class IndexedDf[IoId, T](val df:TypedDf[IoId, T],
                         val indexDf:Df[IoId, (String, Any)]) extends Closeable {

  def apply(i:Long) = df(i)
  def colIds = df.colIds
  def col[T <: Any](id:String)(implicit scope:IoScope) = df.col[T](id)
  def col[T <: Any](i:Long)(implicit scope:IoScope) = df.col[T](i)
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
}

