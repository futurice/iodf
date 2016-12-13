package com.futurice.iodf

import java.io.{BufferedOutputStream, Closeable, DataOutputStream}

import com.futurice.iodf.store._
import com.futurice.iodf.ioseq._
import xerial.larray.buffer.{LBuffer, LBufferAPI}
import xerial.larray.mmap.{MMapBuffer, MMapMemory, MMapMode}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.reflect._
import com.futurice.iodf.Utils._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer

trait IoType[Id, T <: IoObject[Id]] {
  def open(buf:IoData[Id]) : T
  def asTypeOf[E](implicit t:Type) : Option[IoTypeOf[Id, T, E]]

  def writeAny(out:DataOutputStream, data:Any)
  def createAny(file:FileRef[Id], v:Any) = {
    using(new DataOutputStream(file.output)) { out =>
      writeAny(out, v)
    }
    using(file.open) { open(_) }
  }
  def tryCreate[I : TypeTag](file:FileRef[Id], data:I) = {
    asTypeOf[I](typeTag[I].tpe).map { iot =>
      using(new DataOutputStream(file.output)) { out =>
        iot.write(out, data)
      }
      using(file.open) { open(_) }
    }
  }
}

trait WithValueTypeTag[M] {
  def valueTypeTag : TypeTag[M]
}

trait SeqIoType[Id, T <: IoObject[Id], M] extends IoType[Id, T] with WithValueTypeTag[M] {
}

abstract class IoTypeOf[Id, T <: IoObject[Id], In](implicit typ:TypeTag[In]) extends IoType[Id, T] {

  /* Scala creates separate Int types for typeOf[Int] and for case class A {i:Int} members
   * These separate types appear as 'Int' and 'scala.Int' in console outpu
   *
   * This method is a horrible hack for managing these primitive types
   */
  def normalize(t:Type) : Type = {
    t.toString match {
      case "java.lang.String" => typeOf[String]
      case "Boolean" => typeOf[Boolean]
      case _ => t
    }
  }
  def matches(a:Type, b:Type) : Boolean = {
    val (t1, t2) = (normalize(a), normalize(b))
//    System.out.println("comparing " + t1 + " vs " + t2)
    (t1 == t2) || {
      val TypeRef(a, b, c) = t1
      val TypeRef(a2, b2, c2) = t2
      ((a == a2) || (a.toString == "scala.type" && a.toString == a2.toString)) &&
        b == b2 && !c.zip(c2).exists { case (c, c2) => !matches(c, c2) }
    }
  }
  def asTypeOf[E](implicit t:Type) = {
    matches(t, typ.tpe) match {
      case true => Some(this.asInstanceOf[IoTypeOf[Id, T, E]])
      case false => None
    }
/*    System.out.println("comparing " + t + " with " + typ.tpe + " " + (typ.tpe == t))
    val TypeRef(a, b, c) = typ.tpe
    val TypeRef(a2, b2, c2) = t
    System.out.println(" 1.comparing " + a + " with " + a2 + " " + (a == a2))
    System.out.println(" 2.comparing " + b + " with " + b2 + " " + (b == b2))
    System.out.println(" 3.comparing " + c + " with " + c2 + " " + (c == c2))
    t match {
      case TypeRef(a2, b2, c2) if (a==a2 && b==b2 && c==c2)=> Some(this.asInstanceOf[IoTypeOf[Id, T, E]])
      case _ => None
    }*/
  }
/*  def tryCreate[I](id:Id, data:I, dir:Dir[Id]) = {
    Some( create(id, data.asInstanceOf[In], dir) )
  }*/
  def write(output:DataOutputStream, v:In) : Unit
  def writeAny(output:DataOutputStream, v:Any) = {
    v match {
      case i:In => write(output, i)
    }
  }
  def create(file:FileRef[Id], v:In) = {
    using (new DataOutputStream(new BufferedOutputStream(file.output))) { write(_, v) }
    using(file.open) { open(_) }
  }
}

case class IoRef[Id, T <: IoObject[Id]](typ:IoType[Id, _ <: T], dataRef:DataRef[Id]) {
  def open = typ.open(dataRef.open)
}

object IoRef {
  def apply[Id, T <: IoObject[Id]](typ:IoType[Id, T], dir:Dir[Id], id:Id, pos:Long = 0) : IoRef[Id, T]= {
    IoRef(typ, DataRef(dir, id, pos))
  }
}

trait IoObject[Id] extends Closeable {
  def ref : IoRef[Id, _ <: IoObject[Id]]
}

/**
 * This is an object, which pretends to be another object. This is used
 * for adding closed io objects to sequences
 */
case class IoRefObject[Id, T <: IoObject[Id]](override val ref:IoRef[Id, _ <: T]) extends IoObject[Id] {
  override def close(): Unit = {}
}

trait IoIterable[Id, T] extends IoObject[Id] with Iterable[T] {
  def iterator : Iterator[T]
}

trait IoSeq[Id, T] extends IoIterable[Id, T] with PartialFunction[Long, T] {
  // Potentially slow, because O(N) complexity
  def apply(l:Long) : T
  def lsize : Long
  override def size = lsize.toInt
  def isDefinedAt(l:Long) = l >= 0 && l < size
  override def iterator = {
    new Iterator[T] {
      var i = 0L
      override def hasNext: Boolean = {
        i < lsize
      }
      override def next(): T = {
        val rv = apply(i)
        i += 1
        rv
      }
    }
  }
}

/*
trait SortedIoSeq[IoId, ColId <: Ordered[ColId]] extends IoSeq[IoId, ColId] {
}*/

trait Df[IoId, ColId] extends java.io.Closeable {
  def colIds   : IoSeq[IoId, ColId]
  def colIdOrdering : Ordering[ColId]

  def _cols    : IoSeq[IoId, IoSeq[IoId, Any]]

  def colCount = colIds.size
  // size in Long
  def lsize : Long

  def indexOf(id:ColId) =
    Utils.binarySearch(colIds, id)(colIdOrdering)

/*  def indexOf(id:ColId) =
      colIds.iterator.indexOf(id)*/

  def openCol[T <: Any](i:Int) : IoSeq[IoId, T] = {
    _cols(i).asInstanceOf[IoSeq[IoId, T]]
  }
  def apply[T <: Any](i:Int, j:Long) : T = {
    using (openCol[T](i)) { _(j) }
  }
  def openCol[T <: Any](id:ColId) : IoSeq[IoId, T] = {
    indexOf(id) match {
      case -1 => throw new IllegalArgumentException(id + " not found")
      case i => _cols(i).asInstanceOf[IoSeq[IoId, T]]
    }
  }
  def apply[T <: Any](id:ColId, i:Long) : T = {
    using (openCol[T](id)) { _(i) }
  }
}

trait TypedDf[Id, T] extends Df[Id, String] {
  def apply(i:Long) : T
}

class TypedDfView[Id, T](df:Df[Id, String], make:Array[Any] => T)(
  implicit ord:Ordering[String])
  extends TypedDf[Id, T] {

  override def apply(i: Long): T =
    make( (0 until colCount).map { df.apply[Any](_, i) }.toArray)

  override def colIdOrdering = ord

  override def colIds = df.colIds

  override def _cols = df._cols

  override def lsize: Long = df.lsize

  override def close(): Unit = df.close
}

object MathUtils {
  val INV_LOG2 = 1/Math.log(2)
  def log2(v:Double) = Math.log(v) * INV_LOG2

  def eP(f:Long, n:Long, priorP:Double, priorW:Double) = {
    (f + priorP * priorW) / (n + priorW).toDouble
  }
  def h(p:Double) = {
    p * -log2(p) + (1-p) * -log2(1-p)
  }
  def pS(s:Boolean, p:Double) = {
    if (s) p else (1-p)
  }
  def relVarState(relState:Int, v:Int) = {
    (relState & (1 << v)) > 0
  }
  def relStateF(relState:Int, n:Long, fA:Long, fB:Long, fAB:Long) = {
    relState match {
      case 0 => n - fA - fB + fAB
      case 1 => fA - fAB  // a is true
      case 2 => fB - fAB  // b is true
      case 3 => fAB       // a&b are true
    }
  }
}

case class CoStats(n:Long, fA:Long, fB:Long, fAB:Long) {

  def pA = MathUtils.eP(fA, n, 0.5, 2)
  def pB = MathUtils.eP(fB, n, 0.5, 2)

  def hA = MathUtils.h(pA)
  def hB = MathUtils.h(pB)

  val (naivePs, ps) = {
    val (naivePs, ps) = (new Array[Double](4), new Array[Double](4))
    (0 until 4).foreach { s =>
      val pAs = MathUtils.pS(MathUtils.relVarState(s, 0), pA)
      val pBs = MathUtils.pS(MathUtils.relVarState(s, 1), pB)
      val fS = MathUtils.relStateF(s, n, fA, fB, fAB)
      val naive = pAs * pBs
      naivePs(s) = naive
      ps(s) = MathUtils.eP(fS, n, naive, 2)
    }
    (naivePs, ps)
  }
  def d(relState:Int) : Double = {
    ps(relState) / naivePs(relState)
  }
  def d(as:Boolean, bs:Boolean) : Double = d(((if (as) 1 else 0)) + (if (bs) 2 else 0))
  def miPart(relState:Int) = {
    ps(relState) * MathUtils.log2(d(relState))
  }
  def mi = (0 until 4).map(miPart).sum
}

class IndexDfView[IoId, T](val df:TypedDf[IoId, T],
                           val index:Df[IoId, (String, Any)]) extends Closeable {

  def open(idValue:(String, Any)) = {
    index.openCol(idValue).asInstanceOf[DenseIoBits[IoId]]
  }
  def open(i:Int) = {
    index.openCol(i).asInstanceOf[DenseIoBits[IoId]]
  }
  def n = df.lsize
  def f(i:Int) = {
    using(open(i)) { _.bitCount }
  }
  def f(idValue:(String, Any)) = {
    using(open(idValue)) { _.bitCount }
  }
  def co(idValue1:(String, Any), idValue2:(String, Any)) = {
    using(open(idValue1)) { b1 =>
      using (open(idValue2)) { b2 =>
        CoStats(df.lsize, b1.bitCount, b2.bitCount, b1.andCount(b2))
      }
    }
  }
  def co(idValue1:Int, idValue2:Int) = {
    using(open(idValue1)) { b1 =>
      using (open(idValue2)) { b2 =>
        CoStats(df.lsize, b1.bitCount, b2.bitCount, b1.andCount(b2))
      }
    }
  }

  override def close(): Unit = {
    df.close
    index.close
  }
}

trait IoTypes[Id] {
  def orderingOf(valueTypeTag: TypeTag[_]) : Ordering[Any]

  def ioTypeOf(t : Type) : IoTypeOf[Id, _ <: IoObject[Id], _]
  def ioTypeOf[T : TypeTag]() : IoTypeOf[Id, _ <: IoObject[Id], T]
  def writeIoObject[From](t:From, ref:FileRef[Id])(implicit tag:TypeTag[From]) : IoRef[Id, _ <: IoObject[Id]]
  def createIoObject[From](t:From, ref:FileRef[Id])(implicit tag:TypeTag[From]) : IoObject[Id]
  def openIoObject[From](ref:FileRef[Id])(implicit tag:TypeTag[From]) : IoObject[Id]

  def ioTypeId(t:IoType[Id, _]) : Option[Int]
  def idIoType(i:Int) : IoType[Id, _]
}

/*
class VariantType[Id](types:IoTypes[Id]) extends IoTypeOf[Id, IoObject[Id], Any]()(TypeTag.Any) {
  override def create(id: Id, data: Any, dir: Dir[Id]): IoObject[Id] = {

  }
  override def open(id: Id, dir: Dir[Id], pos: Long): IoObject[Id] = {

  }
}*/

object IoTypes {
  type strSeqType = StringIoSeqType[String]

  def apply[Id](types:Seq[IoType[Id, _ <: IoObject[Id]]]) = {
    new IoTypes[Id] {
      def ioTypeOf(t:Type) : IoTypeOf[Id, _ <: IoObject[Id], _] = {
        types.find(_.asTypeOf(t).isDefined).map(_.asTypeOf(t).get) match {
          case Some(v) => v
          case None => throw new RuntimeException("no io type for " + t)
        }
      }
      def ioTypeOf[T : TypeTag]() : IoTypeOf[Id, _ <: IoObject[Id], T] = {
        ioTypeOf(typeOf[T]).asInstanceOf[IoTypeOf[Id, _ <: IoObject[Id], T]]
      }
      def writeIoObject[From : TypeTag](v:From, ref:FileRef[Id]) = {
        val typ = ioTypeOf[From]
        using(new DataOutputStream(new BufferedOutputStream(ref.output))) {
          typ.write(_, v)
        }
        new IoRef[Id, IoObject[Id]](typ, new DataRef[Id](ref.dir, ref.id))
      }
      def openIoObject[From : TypeTag](ref:FileRef[Id]) = {
        using (ref.open) { ioTypeOf[From].open(_) }
      }
      def createIoObject[From : TypeTag](v:From, ref:FileRef[Id]) = {
        writeIoObject(v, ref)
        openIoObject(ref)
      }


      def ioTypeId(t:IoType[Id, _]) = {
        Some(types.indexOf(t)).filter(_ >= 0)
      }
      def idIoType(i:Int) = types(i)

      def orderingOf[T](implicit ord: Ordering[T]) = ord

      def orderingOf(valueTypeTag: TypeTag[_]) = {
        (valueTypeTag.tpe match {
          case t if t == typeOf[Boolean] => orderingOf[Boolean]
          case t if t == typeOf[Int] => orderingOf[Int]
          case t if t == typeOf[String] => orderingOf[String]
        }).asInstanceOf[Ordering[Any]] // TODO: refactor, there has to be a better way
      }
    }
  }
  def strings = {
    val str = new strSeqType
    val buf = new ArrayBuffer[IoType[String, _ <: IoObject[String]]]()
    val self = apply(buf)
    val entryIo = new Serializer[(Int, String, Long)] {
      override def read(b: RandomAccess, pos: Long): (Int, String, Long) = {
        val i = b.getBeInt(pos)
        val l = b.getBeLong(pos+4)
        val s = StringIo.read(b, pos+12)
        (i, s, l)
      }
      override def write(o: DataOutputStream, v: (Int, String, Long)): Unit = {
        o.writeInt(v._1)
        o.writeLong(v._3)
        StringIo.write(o, v._2)
      }
      override def size(o: RandomAccess, pos: Long): Long = {
        4+8+StringIo.size(o, pos+4+8)
      }
    }
    val javaIo = new JavaObjectIo[Any]
    val variantIo = new VariantIo(Array(BooleanIo, IntIo, StringIo), javaIo)
    val tupleIo = new Tuple2Io[String, Any](StringIo, variantIo)
    buf ++=
      Seq(
        str,
        new IntIoArrayType[String],
        new DenseIoBitsType[String](),
        new SparseIoBitsType[String](),
        new IntIoArrayType[String],
        new RefIoSeqType[String,
          IoSeq[String, IoObject[String]]](self,
            new ObjectIoSeqType[String, (Int, String, Long)](entryIo, entryIo)),
        new ObjectIoSeqType[String, (String, Any)](tupleIo, tupleIo),
        new ObjectIoSeqType[String, Any](javaIo, javaIo))
    self
  }
}

case class IndexConf[ColId](analyzers:Map[ColId, Any => Seq[Any]] = Map[ColId, Any => Seq[Any]]()) {
  def analyze(field:ColId, value:Any) = {
    analyzer(field)(value)
  }
  def analyzer(field:ColId) : Any => Seq[Any] = {
    analyzers.getOrElse(field, v => Seq(v))
  }
  def withAnalyzer(field:ColId, analyzer: Any => Seq[Any]) = {
    new IndexConf(analyzers + (field -> analyzer))
  }
  def withoutField(field:ColId) = {
    withAnalyzer(field, v => Seq())
  }
}

object Dfs {
  def strings = new Dfs[String](IoTypes.strings)
}

class Dfs[IoId](types:IoTypes[IoId])(implicit val seqSeqTag : TypeTag[Seq[IoObject[IoId]]]) {

  val l = LoggerFactory.getLogger(this.getClass)

  def apply[ColId](_colIds:IoSeq[IoId, ColId],
                   __cols:IoSeq[IoId, IoSeq[IoId, Any]],
                   _lsize:Long)(implicit ord:Ordering[ColId]) : Df[IoId, ColId] = {
    new Df[IoId, ColId] {
      override def colIds = _colIds
      override def _cols  = __cols
      override def lsize   = _lsize
      override def close(): Unit = {
        _colIds.close; __cols.close
      }
      override def colIdOrdering: Ordering[ColId] = ord
    }
  }

  def create[ColId](
    t:Seq[(ColId, _ <: IoType[IoId, _ <: IoObject[IoId]])],
    cols:Seq[Seq[_ <: Any]],
    dir:Dir[IoId])(
    implicit colIdSeqTag: TypeTag[Seq[ColId]],
    ordering:Ordering[ColId]) : Df[IoId, ColId] = {

    val order = t.map(_._1).zipWithIndex.sortBy(_._1).map(_._2)

    val sz = cols.map(_.size)
    if (sz.min != sz.max) throw new RuntimeException

    val orderedT = order.map(t(_))
    val orderedCols = order.map(cols(_))

    val vd =
      orderedT.zipWithIndex.map(e => (e._2, e._1._2)).zip(orderedCols).map {
        case ((index, vt), data) =>
          vt.createAny(FileRef(dir, dir.id(index+2)), data).asInstanceOf[IoObject[IoId]]
      }
    val ioIds =
      types.createIoObject(
        orderedT.map(_._1),
        FileRef(dir, dir.id(0))).asInstanceOf[IoSeq[IoId, ColId]]
    val ioCols =
      types.createIoObject(
        vd,
        FileRef(dir, dir.id(1))).asInstanceOf[IoSeq[IoId, IoSeq[IoId, Any]]]

    vd.foreach { _.close }

    apply(ioIds, ioCols, orderedCols(0).size)
  }

  def open[ColId](t:Seq[(ColId, _ <: IoType[IoId, _])],
                  dir:Dir[IoId])(implicit colIdSeqTag: TypeTag[Seq[ColId]],
                                 colIdOrdering : Ordering[ColId])
    : Df[IoId, ColId] = {
    val ioIds =
      types.openIoObject[Seq[ColId]](
        FileRef(dir, dir.id(0))).asInstanceOf[IoSeq[IoId, ColId]]
    val ioCols =
      types.openIoObject[Seq[IoObject[IoId]]](
        FileRef(dir, dir.id(1))).asInstanceOf[IoSeq[IoId, IoSeq[IoId, Any]]]

    apply(ioIds, ioCols, using(ioCols(0)) {_.size})
  }

  def indexColIdOrdering[ColId](implicit colOrd: Ordering[ColId]) = {
    new Ordering[(ColId, Any)] {
      override def compare(x: (ColId, Any), y: (ColId, Any)): Int = {
        colOrd.compare(x._1, y._1) match {
          case 0 => // fields matches, so the values should be of the same type
            x._2 match {
              case v:Boolean => v.compare(y._2.asInstanceOf[Boolean])
              case v:Int =>     v.compare(y._2.asInstanceOf[Int])
              case v:String =>  v.compare(y._2.asInstanceOf[String])
            }
          case v => v
        }
      }
    }
  }

  def createIndex[ColId](df:Df[IoId, ColId], dir:Dir[IoId], conf:IndexConf[ColId] = IndexConf[ColId]())
                        (implicit colIdSeqTag: TypeTag[Seq[(ColId, Any)]],
                       boolSeqTag: TypeTag[Seq[Boolean]],
                       ord:Ordering[ColId])= {
    val ids = ArrayBuffer[(ColId, Any)]()
    val cols = ArrayBuffer[IoObject[IoId]]()
    try {
      df.colIds.zipWithIndex.foreach { case (id, i) =>
        using(df.openCol[Any](i)) { col =>
          val ordering = col.ref.typ match {
            case t : WithValueTypeTag[_] =>
              types.orderingOf(t.valueTypeTag)
          }
          val analyzer = conf.analyzer(id)
          val distinct =
            col.par.flatMap(analyzer(_)).toArray.distinct.sorted(ordering)
          val toIndex = distinct.zipWithIndex.toMap
          val indexes = Array.fill(distinct.size)(new ArrayBuffer[Long]())
          col.zipWithIndex.foreach { case (v, i) =>
            analyzer(v).foreach { token =>
              indexes(toIndex(token)) += i
            }
          }
          distinct.zipWithIndex.foreach { case (value, index) =>
            ids += (id -> value)
            val data = indexes(index)
            val before = System.currentTimeMillis()
            val ref =
              types.writeIoObject(
                new SparseBits(data, df.lsize),
                dir.ref(dir.id(cols.size + 2)))
            System.out.println("write took " + (System.currentTimeMillis() - before) + " ms")
            cols +=
              new IoRefObject[IoId, IoObject[IoId]](ref)
          }
        }
      }
      val ioIds =
        types.createIoObject(
          ids: Seq[Any],
          FileRef(dir, dir.id(0))).asInstanceOf[IoSeq[IoId, (ColId, Any)]]
      val ioCols =
        types.createIoObject(
          cols: Seq[IoObject[IoId]],
          FileRef(dir, dir.id(1))).asInstanceOf[IoSeq[IoId, IoSeq[IoId, Any]]]
      apply(ioIds, ioCols, df.lsize)(indexColIdOrdering)
    } finally {
      cols.foreach { _.close }
    }
  }

  def openIndex[ColId](df:Df[IoId, ColId], dir:Dir[IoId])
                       (implicit colIdSeqTag: TypeTag[Seq[(ColId, Any)]],
                        boolSeqTag: TypeTag[Seq[Boolean]],
                        ord:Ordering[ColId]) = {
    val ioIds =
      types.openIoObject[Seq[Any]](
        FileRef(dir, dir.id(0))).asInstanceOf[IoSeq[IoId, (ColId, Any)]]
    val ioCols =
      types.openIoObject[Seq[IoObject[IoId]]](
        FileRef(dir, dir.id(1))).asInstanceOf[IoSeq[IoId, IoSeq[IoId, Any]]]
    apply(ioIds, ioCols, df.lsize)(indexColIdOrdering)
  }
}
case class TypeIoSchema[T](t:Class[_],
                           fields : Seq[(Type, String, _ <: IoType[String, _ <: IoObject[String]])],
                           make:Array[Any] => T) {

  def fieldIoTypes : Seq[(String, _ <: IoType[String, _ <: IoObject[String]])] =
    fields.map(e => (e._2, e._3)).toSeq

  def toColumns(items:Seq[T]) =
    fields.map { case (field, name, vt) =>
      //          System.out.println("matching '" + name + "' with " + t.getMethods.map(e => e.getName + "/" + e.getParameterCount + "/" + (e.getName == name)).mkString(","))
      val accessor =
        t.getMethods.find(m => (m.getName == name)
          && (m.getParameterCount == 0)).get
      items.map { i => accessor.invoke(i) }
    }
}

class StringDfs(types:IoTypes[String]) extends Dfs[String](types) {

  val TypeRef(seqPkg, seqSymbol,_) = typeOf[scala.Seq[String]]

  def typeSchema[T:ClassTag](implicit tag:TypeTag[T]) = {
    val t = classTag[T].runtimeClass

    //    val constructor = t.getConstructors.apply(0)
    val fields =
    tag.tpe.members.filter(!_.isMethod).map { e =>
      (e.typeSignature,
        e.name.decoded.trim(),
        types.ioTypeOf(
          scala.reflect.runtime.universe.internal.typeRef(
            seqPkg, seqSymbol, List(e.typeSignature))))
    }.toArray.sortBy(_._2)

    val constructor =
      t.getConstructors.find(_.getParameterCount == fields.size).get  // fields.map(_._1.getType).toArray :_*)


    val constructorParamNames =
      tag.tpe.members.filter(_.isConstructor).head.asMethod.paramLists.head.map(_.name.decoded.trim).toArray

    val constructorParamIndexes =
      constructorParamNames.map(e => fields.indexWhere(_._2 == e))

    val make = { vs : Array[Any] =>
      constructor.newInstance(constructorParamIndexes.map(vs(_)).asInstanceOf[Array[AnyRef]] : _*).asInstanceOf[T]
    }
    new TypeIoSchema[T](t, fields, make)
  }

  def openTyped[T : ClassTag](dir:Dir[String])(implicit tag:TypeTag[T]) : TypedDf[String, T] = {
    val t = typeSchema[T]
    new TypedDfView[String, T](
      open[String](
        t.fieldIoTypes,
        dir),
      t.make
    )
  }

  def createTyped[T : ClassTag](items:Seq[T], dir:Dir[String])(
    implicit tag:TypeTag[T]) : TypedDf[String, T] = {
    val t = typeSchema[T]

    new TypedDfView[String, T](
      create[String](
        t.fieldIoTypes,
        t.toColumns(items),
        dir),
      t.make)
  }

}

object StringDfs {
  val default = new StringDfs(IoTypes.strings)
}