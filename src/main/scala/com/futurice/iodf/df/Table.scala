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

  /** order to column index */
  def orderToCol : LSeq[Long]
  /** column index to order */
  def colToOrder : LSeq[Long]

  def orderOf(name:String) = colToOrder(indexOfColId(name))

  def withCol[T:TypeTag](colId:String, meta:KeyValue[_]*) = {
    val colType = typeOf[T]
    val colEntries =
      ((colIds zip (colToOrder zip (colTypes zip colMetas))) ++
        Seq((colId, (colToOrder.lsize, (colType, KeyMap(meta : _*))))))
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

  val ColDefaultValue = Key[Any]("colDefaultValue")

  def empty = apply(LSeq.empty, ColSchema.empty)

  def apply() : TableSchema = empty
  def apply(_colOrder:LSeq[Long],
            schema : ColSchema[String],
            closer : Closeable = Utils.dummyCloseable) : TableSchema  = new TableSchema() {
    override lazy val orderToCol =
      LSeq.from(_colOrder.toArray.zipWithIndex.sortBy(_._1).map(_._2.toLong))
    override val colToOrder = _colOrder
    override val colIds   = schema.colIds
    override val colTypes = schema.colTypes
    override val colMetas = schema.colMetas
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
  def withSchema(implicit _schema:TableSchema) = {
    SchemaRow(this, _schema)
  }
  def toDocument(implicit _schema:TableSchema) : Document = {
    withSchema.toDocument
  }
}

trait SchemaRow extends Row {
  def schema : TableSchema
  def get(field:String) : Option[Any]
  def apply(field:String) = get(field).get
  def toDocument : Document = {
    Document(
      this.zipWithIndex.lazyMap { case (v, order) =>
        schema.colIds(schema.orderToCol(order)) -> v
      }.toArray :_*)
  }
}

object SchemaRow {
  def apply(row:Row, _schema:TableSchema) = {
    new SchemaRow {
      override def schema: TableSchema = _schema
      override def lsize: Long = row.lsize
      override def apply(order: Long): Any = row.apply(order)
      override def get(field:String): Option[Any] = {
        val self = this
        schema.colIds.zipWithIndex.find(_._1 == field).map(_._2).map { index =>
          self.apply(index)
        }
      }
    }
  }
}

class RowImpl(values:Seq[Any]) extends Row {
  def apply(order:Long) = values(order.toInt)
  def lsize = values.size
  override def toString = {
    f"Row(${Row.toRowString(values)})"
  }
}

object Row {
  def apply( values : Any*) = from(values)
  def from( values : Seq[Any]) : Row = {
    new RowImpl(values)
  }
  def fromDocument(schema:TableSchema, doc:Document) = {
    Row.from(
      schema.orderToCol.toSeq.map { case col =>
        val colType = schema(col)
        doc.applyOrElse(
          colType.id,
          (id : String) =>
            colType.meta.get(TableSchema.ColDefaultValue).getOrElse {
              throw new IllegalArgumentException(
                "on document value or default value for column " + colType.id + " for document " + doc)
            })
      })
  }
  def toRowString(values:Seq[Any], colWidth:Int = 16, delimiter:String = ", ") = {
    val buf = new StringBuffer()
    val delimSize = delimiter.size
    val w = colWidth + delimSize
    values.zipWithIndex.foreach { case (v, i) =>
      if (buf.length() > 0) buf.append(delimiter)
      val str = v.toString()
      val targetSize = (i+1) * w - delimSize - buf.length()
      val pad = Math.max(0, targetSize - str.length)
      buf.append("".padTo(pad, ' '))
      buf.append(str)
    }
    buf.toString
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

  override val colMetas = schema.colMetas

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
      schema.orderToCol.toArray.map(i => byColIndex(i.toInt))
    Row.from(byOrder)
  }
}

object Table {
  def empty = Table.from(TableSchema.empty, Seq.empty)
  def from(schema:TableSchema, rows:Seq[Row]) : Table = from(schema, LSeq.from(rows))
  def from(schema:TableSchema, rows:LSeq[Row]) : Table = {
    val orderIndex = schema.orderToCol
    val colIds = schema.colIds
    val colTypes = schema.colTypes

/*    val m = runtimeMirror(getClass.getClassLoader)
    val colClasses = colTypes.lazyMap(t => m.runtimeClass(t)).toArray*/
    val cols = Array.fill(colIds.size)(Array.fill[Any](rows.size)(Unit))

    rows.zipWithIndex.foreach { case (row, rowIndex) =>
      if (row.lsize != schema.colCount) throw new IllegalArgumentException(f"row $rowIndex size is ${row.lsize}, while ${schema.lsize} expected. row is ${row}")
      row.zipWithIndex.foreach { case (value, colOrder) =>
        val colIndex = orderIndex(colOrder).toInt
/*        if (!colClasses(colIndex).isAssignableFrom(value.getClass))
          throw new RuntimeException(f"row $rowIndex column ${colIds(colIndex)} at $colOrder value $value of class ${value.getClass} is not compatible with class ${colClasses(colIndex)} for type ${colTypes(colIndex)}")*/
        cols(colIndex)(rowIndex.toInt) = value
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
    dir.ref(0).save(iface.colToOrder)
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

  override def openViewMerged(seqs: Seq[Ref[Table]]): Table = {
    if (seqs.size == 0) {
      Table.empty // is there better way?
    } else {
      val bind = IoScope.open
      val df = bind(dfType.openViewMerged(seqs.map(_.map(_.df))))
      seqs.foreach { e => bind(e.openCopy) }
      Table(seqs.head.get.schema, df, bind)
    }
  }

}