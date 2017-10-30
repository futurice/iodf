package com.futurice.iodf.df

import com.futurice.iodf.IoContext
import com.futurice.iodf._
import com.futurice.iodf.df.MultiCols.DefaultColIdMemRatio
import com.futurice.iodf.io.{DataAccess, DataOutput, MergeableIoType}
import com.futurice.iodf.store.{CfsDir, WrittenCfsDir}
import com.futurice.iodf.util.{KeyMap, LBits, LSeq, Ref}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

trait Indexed[ColId, T <: Cols[ColId]] extends Cols[ColId] with IndexApi[ColId] {
  def df : T
  def indexDf : Index[ColId]

/*  override def openView(from:Long, until:Long) =
    Indexed[ColId, T](df.openView(from, until).asInstanceOf[T], // FIXME: can scala type system help here?
                      indexDf.openView(from, until))
  override def openSelect(indexes:LSeq[Long]) =
    Indexed[ColId, T](df.openSelect(indexes).asInstanceOf[T], // FIXME: can scala type system help here?
                      indexDf.openSelect(indexes))*/
}

object Indexed {
  def viewMerged[ColId, T <: Cols[ColId]: TypeTag](dfs:Seq[Ref[Indexed[ColId, T]]])(
    implicit io:IoContext, tag:TypeTag[Index[ColId]]) : Indexed[ColId, T]= {
    val dfType = io.types.ioTypeOf[T].asInstanceOf[MergeableIoType[T, _ <: T]]
    val indexType = io.types.ioTypeOf[Index[ColId]].asInstanceOf[MergeableIoType[Index[ColId], _ <: ColId]]

    Indexed[ColId, T](
      dfType.viewMerged(dfs.map(_.map(_.df))),
      indexType.viewMerged(dfs.map(_.map(_.indexDf))))
  }

  def from[ColId:Ordering, T <: Cols[ColId]](df:T, conf:IndexConf[ColId]) : Indexed[ColId, T] = {
    Indexed(df, Index.from(df, conf))
  }

  def apply[ColId, T <: Cols[ColId]](df:T,
                                     indexDf:Index[ColId],
                                     closer: () => Unit = () => Unit) : Indexed[ColId, T] = {
    def d = df
    def i = indexDf
    new Indexed[ColId, T] {
      override val df: T = d
      override val indexDf: Index[ColId] = i

      override def close(): Unit = {
        df.close
        indexDf.close
        closer()
      }

      override type ColType[T] = df.ColType[T]

      override def schema = df.schema

      override def _cols: LSeq[_ <: df.ColType[_]] = df._cols

      override def openedIndexes: LSeq[LBits] = indexDf.openedIndexes

      override def lsize: Long = df.lsize

      override def openView(from: Long, until: Long): Indexed[ColId, T] =
        Indexed[ColId, T](
          df.openView(from, until).asInstanceOf[T], // TODO: refactor
          indexDf.openView(from, until))

      override def colIdValues[T](colId: ColId): LSeq[(ColId, T)] =
        indexDf.colIdValues(colId)

      override def colIdValuesWithIndex[T](colId: ColId): Iterable[((ColId, T), Long)] =
        indexDf.colIdValuesWithIndex(colId)

      override def openIndex(idValue: (ColId, Any)): LBits =
        indexDf.openIndex(idValue)

      override def openIndex(i: Long): LBits =
        indexDf.openIndex(i)

      override def colMetas = df.colMetas
    }
  }
}

trait IndexedDf[Row, T <: Df[Row]] extends Df[Row] with Indexed[String, T] {
  override def openView(from: Long, until: Long): Df[Row] = {
    IndexedDf(df.openView(from, until), indexDf.openView(from, until))
  }

  override def openSelect(indexes:LSeq[Long]): Df[Row] = {
    IndexedDf(df.openSelect(indexes), indexDf.openSelect(indexes))
  }

  override def openSelectSome(indexes: LSeq[Option[Long]]) : Df[Option[Row]] = {
    IndexedDf[Option[Row], Df[Option[Row]]](df.openSelectSome(indexes), indexDf.openSelectSome(indexes))
  }
}

object IndexedDf {

  def viewMerged[Row, T <: Df[Row]: TypeTag](dfs:Seq[Ref[IndexedDf[Row, T]]])(
    implicit io:IoContext, tag:TypeTag[Index[String]]) : IndexedDf[Row, T]= {
    val dfType = io.types.ioTypeOf[T].asInstanceOf[MergeableIoType[T, _ <: T]]
    val indexType = io.types.ioTypeOf[Index[String]].asInstanceOf[MergeableIoType[Index[String], _ <: Index[String]]]

    IndexedDf[Row, T](
      dfType.viewMerged(dfs.map(_.map(_.df))),
      indexType.viewMerged(dfs.map(_.map(_.indexDf))))
  }

  def from[Row, T <: Df[Row]](df:T, conf:IndexConf[String]) : IndexedDf[Row, T] = {
    IndexedDf[Row, T](df, Index.from(df, conf))
  }

  def apply[Row, T <: Df[Row]](df:T,
                               indexDf:Index[String],
                               closer: () => Unit = () => Unit) : IndexedDf[Row, T] = {
    def d = df
    def i = indexDf
    new IndexedDf[Row, T] {
      override val df: T = d
      override val indexDf: Index[String] = i

      override def close(): Unit = {
        df.close
        indexDf.close
        closer()
      }

      override type ColType[T] = df.ColType[T]

      override def schema = df.schema

      override def _cols: LSeq[_ <: df.ColType[_]] = df._cols

      override def openedIndexes: LSeq[LBits] = indexDf.openedIndexes

      override def lsize: Long = df.lsize

      override def openView(from: Long, until: Long): IndexedDf[Row, T] =
        IndexedDf[Row, T](
          df.openView(from, until).asInstanceOf[T], // TODO: refactor
          indexDf.openView(from, until))

      override def colIdValues[T](colId: String): LSeq[(String, T)] =
        indexDf.colIdValues(colId)

      override def colIdValuesWithIndex[T](colId: String): Iterable[((String, T), Long)] =
        indexDf.colIdValuesWithIndex(colId)

      override def openIndex(idValue: (String, Any)): LBits =
        indexDf.openIndex(idValue)

      override def openIndex(i: Long): LBits =
        indexDf.openIndex(i)

      override def colMetas = df.colMetas

      override def apply(l: Long) = df(l)

      override def iterator = df.iterator
    }
  }
}


class IndexedIoType[ColId, T <: Cols[ColId]](dfType:MergeableIoType[T, _ <: T],
                                             indexType:IndexIoType[ColId])(
  implicit io:IoContext, tag:TypeTag[Indexed[ColId, T]])
  extends MergeableIoType[Indexed[ColId, T], Indexed[ColId, T]] {

  // Ints are fast and compact
  type CfsFileId = Int
  def DfFileId = 0
  def IndexDfId = 1

  implicit val types = io.types

  override def interfaceType: universe.Type = typeOf[Indexed[ColId, T]]
  override def ioInstanceType: universe.Type = typeOf[Indexed[ColId, T]]

  override def open(data: DataAccess): Indexed[ColId, T] = scoped { implicit bind =>
    val dir = CfsDir.open[CfsFileId](data)
    val df = dfType.open(dir.access(DfFileId))
    val indexDf = indexType.open(dir.access(IndexDfId))
    Indexed(df, indexDf, () => dir.close)
  }

  override def write(out: DataOutput, df: Indexed[ColId, T]): Unit =
    scoped { implicit bind =>
      val dir = bind(WrittenCfsDir.open[CfsFileId](out))
      dfType.save(dir.ref(DfFileId), df.df)
      indexType.save(dir.ref(IndexDfId), df.indexDf)
    }

  /**
    * This is somewhat more memory friendlier alternative, as it only
    * lifts some of the indexes in memory at a time.
    */
  def writeIndexed(out:DataOutput, df:T, indexConf:IndexConf[ColId]) : Unit =
    scoped { implicit bind =>
      val dir = bind(WrittenCfsDir.open[CfsFileId](out))

      val booleanType = typeOf[Boolean]
      dfType.write(out, df)
      indexType.dfType.writeStream(
        out,
        df.lsize,
        Index.indexIterator[ColId](df, indexConf).map { case (colId, bits) =>
          ((colId, booleanType, KeyMap.empty), bits)
        })
    }

  override def viewMerged(seqs: Seq[Ref[Indexed[ColId, T]]]): Indexed[ColId, T] = {
    Indexed[ColId, T](
      dfType.viewMerged(seqs.map(_.map(_.df))),
      indexType.viewMerged(seqs.map(_.map(_.indexDf)))
    )
  }
}

class IndexedDfIoType[Row, T <: Df[Row]](dfType:MergeableIoType[T, _ <: T],
                                         indexType:IndexIoType[String])(
                                          implicit io:IoContext, tag:TypeTag[IndexedDf[Row, T]])
  extends MergeableIoType[IndexedDf[Row, T], IndexedDf[Row, T]] {

  // Ints are fast and compact
  type CfsFileId = Int
  def DfFileId = 0
  def IndexDfId = 1

  implicit val types = io.types

  override def interfaceType: universe.Type = typeOf[IndexedDf[Row, T]]
  override def ioInstanceType: universe.Type = typeOf[IndexedDf[Row, T]]

  override def open(data: DataAccess): IndexedDf[Row, T] = scoped { implicit bind =>
    val dir = CfsDir.open[CfsFileId](data)
    val df = dfType.open(dir.access(DfFileId))
    val indexDf = indexType.open(dir.access(IndexDfId))
    IndexedDf(df, indexDf, () => dir.close)
  }

  override def write(out: DataOutput, df: IndexedDf[Row, T]): Unit =
    scoped { implicit bind =>
      val dir = bind(WrittenCfsDir.open[CfsFileId](out))
      dfType.save(dir.ref(DfFileId), df.df)
      indexType.save(dir.ref(IndexDfId), df.indexDf)
    }

  /**
    * This is somewhat more memory friendlier alternative, as it only
    * lifts some of the indexes in memory at a time.
    */
  def writeIndexed(out:DataOutput, df:T, indexConf:IndexConf[String]) : Unit =
    scoped { implicit bind =>
      val dir = bind(WrittenCfsDir.open[CfsFileId](out))

      val booleanType = typeOf[Boolean]
      dfType.write(out, df)
      indexType.dfType.writeStream(
        out,
        df.lsize,
        Index.indexIterator[String](df, indexConf).map { case (colId, bits) =>
          ((colId, booleanType, KeyMap.empty), bits)
        })
    }

  override def viewMerged(seqs: Seq[Ref[IndexedDf[Row, T]]]): IndexedDf[Row, T] = {
    IndexedDf[Row, T](
      dfType.viewMerged(seqs.map(_.map(_.df))),
      indexType.viewMerged(seqs.map(_.map(_.indexDf)))
    )
  }
}