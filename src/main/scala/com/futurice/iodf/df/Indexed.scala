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

  override def view(from:Long, until:Long) =
    Indexed[ColId, T](df.view(from, until).asInstanceOf[T], // FIXME: can scala type system help here?
                      indexDf.view(from, until))
  override def select(indexes:LSeq[Long]) =
    Indexed[ColId, T](df.select(indexes).asInstanceOf[T], // FIXME: can scala type system help here?
                      indexDf.select(indexes))
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

      override def lsize: Long = df.lsize

      override def view(from: Long, until: Long): Indexed[ColId, T] =
        Indexed[ColId, T](
          df.view(from, until).asInstanceOf[T], // TODO: refactor
          indexDf.view(from, until))

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