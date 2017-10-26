package com.futurice.iodf.df

import java.io.Closeable

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

  def dfRef : Ref[T]
  def df : T = dfRef.get

  def indexDfRef : Ref[Index[ColId]]
  def indexDf : Index[ColId] = indexDfRef.get

  override def openView(from:Long, until:Long) = scoped { implicit bind =>
    Indexed[ColId, T](
      Ref(df.openView(from, until).asInstanceOf[T]), // FIXME: can scala type system help here?
      Ref(indexDf.openView(from, until)))
  }
  override def openSelect(indexes:LSeq[Long]) = scoped { implicit bind =>
    Indexed[ColId, T](
      Ref(df.openSelect(indexes).asInstanceOf[T]), // FIXME: can scala type system help here?
      Ref(indexDf.openSelect(indexes)))
  }
}

object Indexed {
  def openMerged[ColId, T <: Cols[ColId]: TypeTag](dfs:Seq[Ref[Indexed[ColId, T]]])(
    implicit io:IoContext, tag:TypeTag[Index[ColId]]) : Ref[Indexed[ColId, T]] = scoped { implicit bind =>
    val dfType = io.types.ioTypeOf[T].asInstanceOf[MergeableIoType[T, _ <: T]]
    val indexType = io.types.ioTypeOf[Index[ColId]].asInstanceOf[MergeableIoType[Index[ColId], _ <: ColId]]

    Ref.open(
      Indexed[ColId, T](
        dfType.merged(dfs.map(_.map(_.df))),
        indexType.merged(dfs.map(_.map(_.indexDf)))))
  }

  def from[ColId:Ordering, T <: Cols[ColId]](df:T, conf:IndexConf[ColId]) : Indexed[ColId, T] = scoped { implicit bind =>
    Indexed(Ref(df), Ref(Index.from(df, conf)))
  }

  def apply[ColId, T <: Cols[ColId]](df:Ref[T],
                                     indexDf:Ref[Index[ColId]],
                                     closeable: Closeable = Utils.dummyCloseable) : Indexed[ColId, T] = {
    def d = df
    def i = indexDf
    new Indexed[ColId, T] {
      implicit val bind = IoScope.open
      bind(closeable)
      override val dfRef  = d.copy
      override val indexDfRef = i.copy
      override val df = dfRef.get
      override val indexDf = indexDfRef.get

      override def close(): Unit = bind.close
      override type ColType[T] = df.ColType[T]

      override def schema = df.schema

      override def _cols: LSeq[_ <: df.ColType[_]] = df._cols

      override def openedIndexes: LSeq[LBits] = indexDf.openedIndexes

      override def lsize: Long = df.lsize

      override def openView(from: Long, until: Long): Indexed[ColId, T] =
        Indexed[ColId, T](
          Ref.open(df.openView(from, until).asInstanceOf[T]), // TODO: refactor
          Ref.open(indexDf.openView(from, until)))

      override def colIdValues[T](colId: ColId): LSeq[(ColId, T)] =
        indexDf.colIdValues(colId)

      override def colIdValuesWithIndex[T](colId: ColId): Iterable[((ColId, T), Long)] =
        indexDf.colIdValuesWithIndex(colId)

      override def openIndex(idValue: (ColId, Any)): LBits =
        indexDf.openIndex(idValue)

      override def openIndex(i: Long): LBits =
        indexDf.openIndex(i)

      override def colMeta = df.colMeta
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
    val dir = CfsDir[CfsFileId](data)
    Indexed(dfType(dir.access(DfFileId)), indexType(dir.access(IndexDfId)), bind.adopt())
  }

  override def write(out: DataOutput, df: Ref[Indexed[ColId, T]]): Unit =
    scoped { implicit bind =>
      val dir = bind(WrittenCfsDir.open[CfsFileId](out))
      dfType.save(dir.ref(DfFileId), df.get.dfRef)
      indexType.save(dir.ref(IndexDfId), df.get.indexDfRef)
    }

  /**
    * This is somewhat more memory friendlier alternative, as it only
    * lifts some of the indexes in memory at a time.
    */
  def writeIndexed(out:DataOutput, df:Ref[T], indexConf:IndexConf[ColId]) : Unit =
    scoped { implicit bind =>
      val dir = bind(WrittenCfsDir.open[CfsFileId](out))

      val booleanType = typeOf[Boolean]
      dfType.write(out, df)
      indexType.dfType.writeStream(
        out,
        df.get.lsize,
        Index.indexIterator[ColId](df.get, indexConf).map { case (colId, bits) =>
          ((colId, booleanType, KeyMap.empty), bits)
        })
    }

  override def openMerged(seqs: Seq[Ref[Indexed[ColId, T]]]): Indexed[ColId, T] = {
    Indexed[ColId, T](
      dfType.openMerged(seqs.map(_.map(_.df))),
      indexType.openMerged(seqs.map(_.map(_.indexDf)))
    )
  }
}