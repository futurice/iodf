package com.futurice.iodf.df

import java.io.Closeable

import com.futurice.iodf._
import com.futurice.iodf.io.{DataAccess, DataOutput, IoType, MergeableIoType}
import com.futurice.iodf.ioseq.SeqIoType
import com.futurice.iodf.util.{LSeq, Ref}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

trait TableSchema extends DfSchema[String] {

  def orderIndex : LSeq[Long]
  def colOrder : LSeq[Long]

  def withCol[T:TypeTag](colId:String) = {
    val colType = typeOf[T]
    val colEntries =
      ((colIds zip (colOrder zip colTypes)) ++
        Seq((colId, (colOrder.lsize, colType))))
        .toArray.sortBy(_._1)

    TableSchema(
      LSeq.from(colEntries.map(_._2._1)),
      LSeq.from(colEntries.map(_._1)),
      LSeq.from(colEntries.map(_._2._2)))
  }

}

object TableSchema {
  def apply(_colOrder:LSeq[Long] = LSeq.empty,
            _colIds:LSeq[String] = LSeq.empty,
            _colTypes:LSeq[Type]= LSeq.empty) = new TableSchema() {
    override lazy val orderIndex =
      LSeq.from(_colOrder.toArray.zipWithIndex.sortBy(_._1).map(_._2.toLong))
    override val colOrder = _colOrder
    override val colIds   = _colIds
    override val colTypes = _colTypes
  }
  def from(_colOrder:LSeq[Long], df:Df[String]) = {
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
class Table(val schema:TableSchema, val df:Df[String]) extends Df[String] with LSeq[Row] {

  override type ColType[T] = LSeq[T]

  override val colIds: LSeq[String] = schema.colIds

  override val colTypes: LSeq[universe.Type] = schema.colTypes

  override def colIdOrdering: Ordering[String] = implicitly[Ordering[String]]

  override def _cols: LSeq[_ <: LSeq[_]] = df._cols

  override def lsize: Long = df.lsize

  override def size : Int = df.size

  override def view(from: Long, until: Long): Table =
    Table(schema, df.view(from, until))

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
  def from(schema:TableSchema, rows:Array[Row]) = {
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
          Df(colIds,
             colTypes,
             new LSeq[LSeq[_]] {
               def apply(i:Long) = LSeq.from(cols(i.toInt))
               def lsize = colIds.size
             },
             sz))
  }
  def apply(schema:TableSchema, df:Df[String]) = {
    new Table(schema, df)
  }
}

class TableIoType(longType:SeqIoType[Long, LSeq[Long], _ <: LSeq[Long]],
                  dfType:DfIoType[String]) extends MergeableIoType[Table, Table] {

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
    // FIXME: this is somewhat problematic situation, because
    //        we cannot access the schema. How can we handle situations
    //        with empty merge list?
    Table(seqs.head.get.schema, dfType.viewMerged(seqs.map(_.map(_.df))))
  }

}