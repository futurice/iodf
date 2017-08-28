package com.futurice.iodf.df

import java.io.Closeable

import com.futurice.iodf._
import com.futurice.iodf.io.{DataAccess, DataOutput, IoType, MergeableIoType}
import com.futurice.iodf.ioseq.SeqIoType
import com.futurice.iodf.util._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

trait TableSchema extends ColSchema[String] {

  def orderIndex : LSeq[Long]
  def colOrder : LSeq[Long]

  def withCol[T:TypeTag](colId:String, meta:KeyValue[_]*) = {
    val colType = typeOf[T]
    val colEntries =
      ((colIds zip (colOrder zip (colTypes zip colMeta))) ++
        Seq((colId, (colOrder.lsize, (colType, KeyMap(meta : _*))))))
        .toArray.sortBy(_._1)

    TableSchema(
      LSeq.from(colEntries.map(_._2._1)),
      LSeq.from(colEntries.map(_._1)),
      LSeq.from(colEntries.map(_._2._2._1)),
      LSeq.from(colEntries.map(_._2._2._2)))
  }

}

object TableSchema {
  def apply(_colOrder:LSeq[Long] = LSeq.empty,
            _colIds:LSeq[String] = LSeq.empty,
            _colTypes:LSeq[Type] = LSeq.empty,
            _colMeta:LSeq[KeyMap] = LSeq.empty) = new TableSchema() {
    override lazy val orderIndex =
      LSeq.from(_colOrder.toArray.zipWithIndex.sortBy(_._1).map(_._2.toLong))
    override val colOrder = _colOrder
    override val colIds   = _colIds
    override val colTypes = _colTypes
    override val colMeta = _colMeta
  }
  def from(_colOrder:LSeq[Long], df:Cols[String]) = {
    apply(_colOrder, df.colIds, df.colTypes)
  }
}

trait Row extends LSeq[Any] {
  def apply(order:Long) : Any
  def lsize : Long
}

object Row {
  def apply( values : Any*) = from(values)
  def from( values : Seq[Any]) = {
    new Row {
      def apply(order:Long) = values(order.toInt)
      def lsize = values.size
      override def toString =
        f"Row(${values.map(v => f"$v%16s").mkString(", ")})"
    }
  }
}

/**
  * Created by arau on 11.7.2017.
  */
class Table(val schema:TableSchema, val df:Cols[String]) extends Df[Row] {

  override type ColType[T] = LSeq[T]

  override val colIds: LSeq[String] = schema.colIds

  override val colTypes: LSeq[universe.Type] = schema.colTypes

  override val colMeta = schema.colMeta

  override def colIdOrdering: Ordering[String] = implicitly[Ordering[String]]

  override def _cols: LSeq[_ <: LSeq[_]] = df._cols

  override def lsize: Long = df.lsize

  override def size : Int = df.size

  override def view(from: Long, until: Long): Table =
    Table(schema, df.view(from, until))
  override def select(indexes:LSeq[Long]) =
    Table(schema, df.select(indexes))

  override def close(): Unit = df.close

  override def apply(i:Long) = {
    val byColIndex =
      _cols.map { openedCol : LSeq[Any] =>
        using (openedCol) { _(i) }
      }.toArray
    val byOrder : Array[Any] =
      schema.orderIndex.toArray.map(i => byColIndex(i.toInt))
    Row.from(byOrder)
  }
}

object Table {
  def empty = Table.from(TableSchema(), Seq.empty)
  def from(schema:TableSchema, rows:Seq[Row]) = {
    val orderIndex = schema.orderIndex
    val colIds = schema.colIds
    val colTypes = schema.colTypes
    val cols = Array.fill(colIds.size)(Array.fill[Any](rows.size)(Unit))

    rows.zipWithIndex.foreach { case (row, rowIndex) =>
      row.zipWithIndex.foreach { case (value, colOrder) =>
        cols(orderIndex(colOrder).toInt)(rowIndex) = value
      }
    }

    val sz = rows.size.toLong
    apply(schema,
          Cols(colIds,
             colTypes,
             schema.colMeta,
             new LSeq[LSeq[_]] {
               def apply(i:Long) = LSeq.from(cols(i.toInt))
               def lsize = colIds.size
             },
             sz))
  }
  def apply(schema:TableSchema, df:Cols[String]) = {
    new Table(schema, df)
  }
}

class TableIoType(longType:SeqIoType[Long, LSeq[Long], _ <: LSeq[Long]],
                  dfType:ColsIoType[String]) extends MergeableIoType[Table, Table] {

  override def interfaceType: universe.Type = typeOf[Table]

  override def ioInstanceType: universe.Type = typeOf[Table]

  override def open(ref: DataAccess): Table = scoped { implicit bind =>
    val dfPos = ref.getBeLong(ref.size-8)
    val colOrder = bind(longType.open(ref.view(0, dfPos)))
    val df = dfType.open(ref.view(dfPos, ref.size-8))
    new Table(
      TableSchema.from(LSeq.from(colOrder.toArray), df), df)
  }

  override def write(out: DataOutput, iface: Table): Unit = {
    val begin = out.pos
    longType.write(out, iface.schema.colOrder)
    val dfPos = out.pos - begin
    dfType.write(out, iface.df)
    out.writeLong(dfPos)
  }

  override def viewMerged(seqs: Seq[Ref[Table]]): Table = {
    if (seqs.size == 0) {
      Table.empty // is there better way?
    } else {
      Table(seqs.head.get.schema, dfType.viewMerged(seqs.map(_.map(_.df))))
    }
  }

}