package com.futurice.iodf.df

import java.io.Closeable

import com.futurice.iodf._
import com.futurice.iodf.io.{DataAccess, DataOutput, IoType, MergeableIoType}
import com.futurice.iodf.ioseq.SeqIoType
import com.futurice.iodf.store.{OrderDir, WrittenOrderDir}
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
      ColSchema(
        LSeq.from(colEntries.map(_._1)),
        LSeq.from(colEntries.map(_._2._2._1)),
        LSeq.from(colEntries.map(_._2._2._2))))
  }

}

object TableSchema {
  def empty = apply(LSeq.empty, ColSchema.empty)
  def apply() : TableSchema = empty
  def apply(_colOrder:LSeq[Long],
            schema :ColSchema[String],
            closer : Closeable = Utils.dummyCloseable) : TableSchema  = new TableSchema() {
    override lazy val orderIndex =
      LSeq.from(_colOrder.toArray.zipWithIndex.sortBy(_._1).map(_._2.toLong))
    override val colOrder = _colOrder
    override val colIds   = schema.colIds
    override val colTypes = schema.colTypes
    override val colMeta = schema.colMeta
    override val colIdOrdering = schema.colIdOrdering
    override def close = closer.close
  }
  def from(_colOrder:LSeq[Long], df:Cols[String]) = {
    apply(_colOrder, df.schema)
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
class Table(val schema:TableSchema, val df:Cols[String], closer:Closeable = Utils.dummyCloseable) extends Df[Row] {

  Tracing.opened(this)

  override type ColType[T] = LSeq[T]

  override val colIds: LSeq[String] = schema.colIds

  override val colTypes: LSeq[universe.Type] = schema.colTypes

  override val colMeta = schema.colMeta

  override def colIdOrdering: Ordering[String] = implicitly[Ordering[String]]

  override def _cols: LSeq[_ <: LSeq[_]] = df._cols

  override def lsize: Long = df.lsize

  override def size : Int = df.size

  override def openView(from: Long, until: Long): Table =
    Table(schema, df.openView(from, until))
  override def openSelect(indexes:LSeq[Long]) =
    Table(schema, df.openSelect(indexes))

  override def close(): Unit = {
    Tracing.closed(this)
    closer.close
  }

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
  def empty = Table.from(TableSchema.empty, Seq.empty)
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
          Cols(
            schema,
            new LSeq[LSeq[_]] {
              def apply(i:Long) = LSeq.from(cols(i.toInt))
              def lsize = colIds.size
            },
            sz))
  }
  def apply(schema:TableSchema, df:Cols[String], closer:Closeable = Utils.dummyCloseable) = {
    new Table(schema, df, closer)
  }
}

class TableSchemaIoType(implicit io:IoContext) extends IoType[TableSchema, TableSchema] {
  override def interfaceType: universe.Type = typeOf[TableSchema]
  override def ioInstanceType: universe.Type = typeOf[TableSchema]

  override def open(ref: DataAccess): TableSchema = scoped { implicit bind =>
    val dir = OrderDir(ref)
    val colOrder = dir.ref(0).as[LSeq[Long]]
    val colSchema = dir.ref(1).as[ColSchema[String]]
    TableSchema(colOrder, colSchema, bind.adopt)
  }

  override def write(out: DataOutput, iface: TableSchema): Unit = scoped { implicit bind =>
    val dir = WrittenOrderDir(out)
    dir.ref(0).save(iface.colOrder)
    dir.ref(1).save(iface : ColSchema[String])
  }
}

class TableIoType(implicit io:IoContext) extends MergeableIoType[Table, Table] {

  override def interfaceType: universe.Type = typeOf[Table]

  override def ioInstanceType: universe.Type = typeOf[Table]

  override def open(ref: DataAccess): Table = scoped { implicit bind =>
    val dir = OrderDir(ref)
    val schema = dir.ref(0).as[TableSchema]
    val cols = dir.ref(1).as[Cols[String]]
    new Table(schema, cols, bind.adopt())
  }

  override def write(out: DataOutput, iface: Table): Unit = scoped { implicit bind =>
    val dir = WrittenOrderDir(out)
    dir.ref(0).save(iface.schema)
    dir.ref(1).save(iface.df)
  }

  lazy val dfType =
    io.types.ioTypeOf[Cols[String]].asInstanceOf[MergeableIoType[Cols[String], _ <: Cols[String]]]

  override def viewMerged(seqs: Seq[Ref[Table]]): Table = {
    if (seqs.size == 0) {
      Table.empty // is there better way?
    } else {
      val bind = IoScope.open
      val df = bind(dfType.viewMerged(seqs.map(_.map(_.df))))
      seqs.foreach { e => bind(e.openCopy) }
      Table(seqs.head.get.schema, df, bind)
    }
  }

}