package com.futurice.iodf

import java.io._
import java.util

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
    using(new DataOutputStream(file.openOutput)) { out =>
      writeAny(out, v)
    }
    using(file.open) { open(_) }
  }
  def tryCreate[I : TypeTag](file:FileRef[Id], data:I) = {
    asTypeOf[I](typeTag[I].tpe).map { iot =>
      using(new DataOutputStream(file.openOutput)) { out =>
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
  def defaultSeq(lsize:Long) : Any = {
    throw new RuntimeException("not implemented")
  }
  def writeMerged(out:DataOutputStream, seqA:T, seqB:T) : Unit
  def writeAnyMerged(out:DataOutputStream, seqA:Any, seqB:Any) = {
    writeMerged(out, seqA.asInstanceOf[T], seqB.asInstanceOf[T])
  }
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
    //System.out.println("comparing " + t1 + "/" + t1.hashCode()+ " vs " + t2 + "/" + t2.hashCode())
    val rv = (t1 == t2) || {
      val TypeRef(a, b, c) = t1
      val TypeRef(a2, b2, c2) = t2
/*      System.out.println("  " + a + "/" + a.hashCode()+ " vs " + a2 + "/" + a2.hashCode())
      System.out.println("  " + b + "/" + b.hashCode()+ " vs " + b2 + "/" + b2.hashCode())
      (0 until Math.min(c.size, c2.size)).foreach { i =>
        System.out.println("  " + c(i) + "/" + c(i).hashCode() + " vs " + c2(i) + "/" + c2(i).hashCode())
      }*/
      ((a == a2) || (a.toString == a2.toString)) &&
        b == b2 && !c.zip(c2).exists { case (c, c2) => !matches(c, c2) }
    }
    //System.out.println("  -> " + rv)
    rv
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
    using (new DataOutputStream(new BufferedOutputStream(file.openOutput))) { write(_, v) }
    using(file.open) { open(_) }
  }
}

case class IoRef[Id, T <: IoObject[Id]](typ:IoType[Id, _ <: T], dataRef:DataRef[Id]) {
  def open = typ.open(dataRef.open)
}

object IoRef {
  def apply[Id, T <: IoObject[Id]](typ:IoType[Id, T], dir:Dir[Id], id:Id, pos:Long = 0, size:Option[Long]) : IoRef[Id, T]= {
    IoRef(typ, DataRef(dir, id, pos, size))
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

  def col[T <: Any](id: ColId)(implicit scope:IoScope) = {
    scope.bind(openCol[T](id))
  }
  def col[T <: Any](i: Long)(implicit scope:IoScope) = {
    scope.bind(openCol[T](i))
  }
  def openCol[T <: Any](i:Long) : IoSeq[IoId, T] = {
    _cols(i).asInstanceOf[IoSeq[IoId, T]]
  }
  def apply[T <: Any](i:Long, j:Long) : T = {
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
/*
class MultiDf[IoId, ColId](dfs:Array[Df[IoId, ColId]])(implicit val colIdOrdering:Ordering[ColId]) extends Df[IoId, ColId] {

  override val colIds: IoSeq[IoId, ColId] = {
    new IoSeq[IoId, ColId]{
      // Potentially slow, because O(N) complexity
      override def apply(l: Long): ColId = ???

      override def lsize: Long = ???

      override def ref: IoRef[IoId, _ <: IoObject[IoId]] = ???

      override def close(): Unit = ???
    }
  }

  override val _cols: IoSeq[IoId, IoSeq[IoId, Any]] =
    new IoSeq[IoId, IoSeq[IoId, Any]]{
      // Potentially slow, because O(N) complexity
      override def apply(l: Long): IoSeq[IoId, Any] = ???

      override def lsize: Long = ???

      override def ref: IoRef[IoId, _ <: IoObject[IoId]] = ???

      override def close(): Unit = ???
    }

  // size in Long
  override lazy val lsize: Long = dfs.map(_.lsize).sum

  override def close(): Unit = ???
}
*/
trait TypedDf[IoId, T] extends Df[IoId, String] {
  def apply(i:Long) : T
  def thisColIndex : Int
  def fieldNames : Iterable[String]
  def fieldIndexes : Iterable[Int]
}

class TypedDfView[IoId, T](df:Df[IoId, String])(
  implicit ord:Ordering[String])
  extends TypedDf[IoId, T] {

  lazy val thisColId = indexOf("this")

  override def apply(i: Long): T =
    df.apply[Object](thisColId, i).asInstanceOf[T]

  override def colIdOrdering = ord

  override def colIds = df.colIds

  lazy val thisColIndex = indexOf("this")
  override def fieldNames = colIds.filter(_ != "this")
  override def fieldIndexes = (0 until colIds.size).filter(_ != thisColIndex)

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
  def l(p:Double) = -log2(p)
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
object CoStats {
  def apply(a: IoBits[_], b: IoBits[_]) : CoStats = {
    CoStats(a.lsize, a.f, b.f, a.fAnd(b))
  }
}


class IndexedDf[IoId, T](val df:TypedDf[IoId, T],
                         val indexDf:Df[IoId, (String, Any)]) extends Closeable {

  def apply(i:Long) = df(i)
  def colIds = df.colIds
  def col[T <: Any](id:String)(implicit scope:IoScope) = df.col[T](id)
  def col[T <: Any](i:Long)(implicit scope:IoScope) = df.col[T](i)
  def index(idValue:(String, Any))(implicit scope:IoScope) : IoBits[IoId] = {
    scope.bind(openIndex(idValue))
  }
  def index(i:Int)(implicit scope:IoScope) : IoBits[IoId] = {
    scope.bind(openIndex(i))
  }
  def openIndex(idValue:(String, Any)) : IoBits[IoId] = {
    indexDf.indexOf(idValue) match {
      case -1 => new EmptyIoBits[IoId](indexDf.lsize)
      case i => openIndex(i)
    }
  }
  def openIndex(i:Int) : IoBits[IoId] = {
    indexDf.openCol(i).asInstanceOf[IoBits[IoId]]
  }

  def lsize = df.lsize

  def n = df.lsize
  def f(i:Int) = {
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

trait IoTypes[IoId] {
  def orderingOf(valueTypeTag: TypeTag[_]) : Ordering[Any]

  def ioTypeOf(t : Type) : IoTypeOf[IoId, _ <: IoObject[IoId], _]
  def ioTypeOf[T : TypeTag]() : IoTypeOf[IoId, _ <: IoObject[IoId], T]
  def writeIoObject[From](t:From, ref:FileRef[IoId])(implicit tag:TypeTag[From]) : IoRef[IoId, _ <: IoObject[IoId]]
  def createIoObject[From](t:From, ref:FileRef[IoId])(implicit tag:TypeTag[From]) : IoObject[IoId]
  def openIoObject[From](ref:FileRef[IoId])(implicit tag:TypeTag[From]) : IoObject[IoId]

  def ioTypeId(t:IoType[IoId, _]) : Option[Int]
  def idIoType(i:Int) : IoType[IoId, _]

  def intToId(i:Int) : IoId

  // shortcuts: reconsider
  def idOrdering : Ordering[IoId]
  def anyOrdering : Ordering[Any]
  def idSeqType : SeqIoType[IoId, _ <: IoSeq[IoId, IoId], IoId]
  def longSeqType: SeqIoType[IoId, _ <: IoSeq[IoId, Long], Long]

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

      def apply[Id](types:Seq[IoType[Id, _ <: IoObject[Id]]],
                    _idIo:Serializer[Id],
                    _intToId: Int => Id)(implicit idTag:TypeTag[Id], idOrd: Ordering[Id]) = {
        new IoTypes[Id] {
          def ioTypeOf(t:Type) : IoTypeOf[Id, _ <: IoObject[Id], _] = {
            types.find(_.asTypeOf(t).isDefined).map(_.asTypeOf(t).get) match {
              case Some(v) => v
              case None =>
                throw new RuntimeException("no io type for " + t + " / " + t.hashCode() + ". ")
            }
          }
          def ioTypeOf[T : TypeTag]() : IoTypeOf[Id, _ <: IoObject[Id], T] = {
            ioTypeOf(typeOf[T]).asInstanceOf[IoTypeOf[Id, _ <: IoObject[Id], T]]
          }
          def writeIoObject[From : TypeTag](v:From, ref:FileRef[Id]) = {
            val typ = ioTypeOf[From]
            using(new DataOutputStream(new BufferedOutputStream(ref.openOutput))) {
              typ.write(_, v)
            }
            new IoRef[Id, IoObject[Id]](typ, new DataRef[Id](ref.dir, ref.id))
          }
          def openIoObject[From](ref:FileRef[Id])(implicit tag:TypeTag[From]) = {
            using (ref.open) { ioTypeOf[From].open(_) }
          }
          def createIoObject[From](v:From, ref:FileRef[Id])(implicit tag:TypeTag[From]) = {
            writeIoObject(v, ref)
            openIoObject(ref)(tag)
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
              case t if t == typeOf[Long] => orderingOf[Long]
              case t if t == typeOf[String] => orderingOf[String]
              case _ =>
                throw new IllegalArgumentException(valueTypeTag.tpe + " is unknown")
            }).asInstanceOf[Ordering[Any]] // TODO: refactor, there has to be a better way
          }
          def idOrdering = idOrd
          def anyOrdering : Ordering[Any] = new Ordering[Any] {
            val b = orderingOf[Boolean]
            val i = orderingOf[Int]
            val l = orderingOf[Long]
            val s = orderingOf[String]
            override def compare(x: Any, y: Any): Int = {
              (x, y) match {
                case (xv: Boolean, yv:Boolean) => b.compare(xv, yv)
                case (xv: Int, yv:Int) => i.compare(xv, yv)
                case (xv: Long, yv:Long) => l.compare(xv, yv)
                case (xv: String, yv:String) => s.compare(xv, yv)
              }
            }
          }
          def idSeqType : SeqIoType[Id, _ <: IoSeq[Id, Id], Id] = {
            ioTypeOf[Seq[Id]].asInstanceOf[SeqIoType[Id, _ <: IoSeq[Id, Id], Id]]
          }
          def longSeqType : SeqIoType[Id, _ <: IoSeq[Id, Long], Long] = {
            ioTypeOf[Seq[Long]].asInstanceOf[SeqIoType[Id, _ <: IoSeq[Id, Long], Long]]
          }

          override def intToId(i: Int): Id = _intToId(i)
        }
      }
      def strings = {
        val str = new strSeqType
        val buf = new ArrayBuffer[IoType[String, _ <: IoObject[String]]]()
        val self = apply(buf, StringIo, "_" + _.toString)
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
        val variantIo = new VariantIo(Array(BooleanIo, IntIo, LongIo, StringIo), javaIo)
        val tupleIo = new Tuple2Io[String, Any](StringIo, variantIo)
        buf ++=
          Seq(
            str,
            new IntIoArrayType[String],
            new LongIoArrayType[String],
            new DenseIoBitsType[String](),  // converts Seq[Boolean]
            new DenseIoBitsType2[String](), // converts BitSet
            new SparseIoBitsType[String](),
            //        new SparseToDenseIoBitsType[String](),
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
    def default = new Dfs[String](IoTypes.strings)
  }

class Dfs[IoId : ClassTag](types:IoTypes[IoId])(implicit val seqSeqTag : TypeTag[Seq[IoObject[IoId]]]) {

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

  def writeMerged[ColId](a:Df[IoId,ColId], b:Df[IoId, ColId], dir:Dir[IoId])(
      implicit colIdSeqTag: TypeTag[Seq[ColId]],
      ordering:Ordering[ColId]) : Unit = {
    val newColIds = new ArrayBuffer[ColId]()
    val aIndex = new ArrayBuffer[Long]()
    val bIndex = new ArrayBuffer[Long]()
    val aIds = a.colIds
    val bIds = b.colIds
    var aAt = 0L
    var bAt = 0L
    while (aAt < aIds.size && bAt < bIds.size) {
      val aId = aIds(aAt)
      val bId = bIds(bAt)
      val o = ordering.compare(aId, bId)
      if (o == 0) {
        aIndex += aAt
        bIndex += bAt
        newColIds += aId
        aAt += 1
        bAt += 1
      } else if (o < 0) {
        aIndex += aAt
        bIndex += -1
        newColIds += aId
        aAt += 1
      } else {
        aIndex += -1
        bIndex += bAt
        newColIds += bId
        bAt += 1
      }
    }
    while (aAt < aIds.size) {
      aIndex += aAt
      bIndex += -1
      newColIds += aIds(aAt)
      aAt += 1
    }
    while (bAt < bIds.size) {
      aIndex += -1
      bIndex += bAt
      newColIds += bIds(bAt)
      bAt += 1
    }

    val vd =
      (0 until newColIds.size).map { index =>
        val fileRef = FileRef(dir, dir.id(index+2))
        val vt =
          using (new DataOutputStream(fileRef.openOutput)) { out =>
            (aIndex(index), bIndex(index)) match {
              case (-1, bi) =>
                using(b.openCol(bi)) { bSeq =>
                  val t = bSeq.ref.typ.asInstanceOf[SeqIoType[IoId, _ <: IoSeq[IoId,Any], _]]
                  t.writeAnyMerged(out, t.defaultSeq(a.lsize), bSeq)
                  t
                }
              case (ai, -1) =>
                using(a.openCol(ai)) { aSeq =>
                  val t = aSeq.ref.typ.asInstanceOf[SeqIoType[IoId, _ <: IoSeq[IoId,Any], _]]
                  t.writeAnyMerged(out, aSeq, t.defaultSeq(b.lsize))
                  t
                }
              case (ai, bi)  =>
                using(a.openCol(ai)) { aSeq =>
                  using(b.openCol(bi)) { bSeq =>
                    val t = aSeq.ref.typ.asInstanceOf[SeqIoType[IoId, _ <: IoSeq[IoId,Any], _]]
                    t.writeAnyMerged(out, aSeq, bSeq)
                    t
                  }
                }
            }
          }
        new IoRefObject[IoId, IoSeq[IoId, Any]](
          IoRef(vt, fileRef.toDataRef)) : IoObject[IoId]
      }
    try {
      types.writeIoObject(
        newColIds : Seq[ColId],
        FileRef(dir, dir.id(0)))
      types.writeIoObject(
        vd : Seq[IoObject[IoId]],
        FileRef(dir, dir.id(1)))
    } finally {
      vd.foreach {
        _.close
      }
    }
  }

  def writeSeq[T](vs:Seq[T], out:DataOutputStream)(implicit seqTag: TypeTag[Seq[T]]) = {
    val t = types.ioTypeOf[Seq[T]]
    t.write(out, vs)
    t
  }

  def openSeq[T](ref:DataRef[IoId])(implicit seqTag: TypeTag[Seq[T]]) = {
    val t = types.ioTypeOf[Seq[T]]
    using (ref.open) { d =>
      t.open(d).asInstanceOf[IoSeq[IoId, T]]
    }
  }

  def write[ColId](t:Seq[(ColId, _ <: IoType[IoId, _ <: IoObject[IoId]])],
                   cols:Seq[Seq[_ <: Any]],
                   dir:Dir[IoId])(
                    implicit colIdSeqTag: TypeTag[Seq[ColId]],
                    ordering:Ordering[ColId]) : Unit = {

    val order = t.map(_._1).zipWithIndex.sortBy(_._1).map(_._2)

    val sz = cols.map(_.size)
    if (sz.min != sz.max) throw new RuntimeException

    val orderedT = order.map(t(_))
    val orderedCols = order.map(cols(_))

    val vd =
      orderedT.zipWithIndex.map(e => (e._2, e._1._2)).zip(orderedCols).map {
        case ((index, vt), data) =>
          val fileRef = FileRef(dir, dir.id(index+2))
          using (new DataOutputStream(fileRef.openOutput)) { out =>
            vt.writeAny(out, data)
          }
          new IoRefObject[IoId, IoSeq[IoId, Any]](
            IoRef(vt, fileRef.toDataRef)) : IoObject[IoId]
      }
    try {
      types.writeIoObject(
        orderedT.map(_._1),
        FileRef(dir, dir.id(0)))
      types.writeIoObject(
        vd,
        FileRef(dir, dir.id(1)))
    } finally {
      vd.foreach {
        _.close
      }
    }
  }

  def create[ColId](t:Seq[(ColId, _ <: IoType[IoId, _ <: IoObject[IoId]])],
                    cols:Seq[Seq[_ <: Any]],
                    dir:Dir[IoId])(
                    implicit colIdSeqTag: TypeTag[Seq[ColId]],
                    ordering:Ordering[ColId]) : Df[IoId, ColId] = {
    write(t, cols, dir)
    open(dir)
    /*
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

        apply(ioIds, ioCols, orderedCols(0).size)*/
  }

  def open[ColId](dir:Dir[IoId])(implicit colIdSeqTag: TypeTag[Seq[ColId]],
                                 colIdOrdering : Ordering[ColId])
  : Df[IoId, ColId] = {
    val ioIds =
      types.openIoObject[Seq[ColId]](
        FileRef(dir, dir.id(0))).asInstanceOf[IoSeq[IoId, ColId]]
    val ioCols =
      types.openIoObject[Seq[IoObject[IoId]]](
        FileRef(dir, dir.id(1))).asInstanceOf[IoSeq[IoId, IoSeq[IoId, Any]]]
    val sz = using(ioCols(0)) {_.lsize}
    apply(ioIds, ioCols, sz)
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
                         ord:Ordering[ColId]) = {
    writeIndex(df, dir, conf)
    openIndex(df, dir)
  }

  def writeIndex[ColId](df:Df[IoId, ColId], dir:Dir[IoId], conf:IndexConf[ColId] = IndexConf[ColId]())
                       (implicit colIdSeqTag: TypeTag[Seq[(ColId, Any)]],
                        boolSeqTag: TypeTag[Seq[Boolean]],
                        ord:Ordering[ColId])= {
    val ids = ArrayBuffer[(ColId, Any)]()
    val cols = ArrayBuffer[IoObject[IoId]]()
    try {
      df.colIds.zipWithIndex.filter(_._1 != "this").foreach { case (id, i) =>
        using(df.openCol[Any](i)) { col =>
          val ordering = col.ref.typ match {
            case t : WithValueTypeTag[_] =>
              types.orderingOf(t.valueTypeTag)
          }
          val analyzer = conf.analyzer(id)
          val distinct =
            col.toArray.flatMap(analyzer(_)).distinct.sorted(ordering)
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
              if (data.size * 1024 > df.lsize) {
                val bitset = new util.BitSet(df.lsize.toInt)
                data.foreach { i => bitset.set(i.toInt) }
                types.writeIoObject(
                  new DenseBits(bitset, df.lsize),
                  dir.ref(dir.id(cols.size + 2)))
              } else {
                types.writeIoObject(
                  new SparseBits(data, df.lsize),
                  dir.ref(dir.id(cols.size + 2)))
              }

            //System.out.println("write took " + (System.currentTimeMillis() - before) + " ms")
            cols +=
              new IoRefObject[IoId, IoObject[IoId]](ref)
          }
        }
      }
      types.writeIoObject(
        ids: Seq[(ColId,Any)],
        FileRef(dir, dir.id(0)))
      types.writeIoObject(
        cols: Seq[IoObject[IoId]],
        FileRef(dir, dir.id(1)))
    } finally {
      cols.foreach { _.close }
    }
  }

  def openIndex[ColId](df:Df[IoId, ColId], dir:Dir[IoId])
                      (implicit colIdSeqTag: TypeTag[Seq[(ColId, Any)]],
                       boolSeqTag: TypeTag[Seq[Boolean]],
                       ord:Ordering[ColId]) = {
    val ioIds =
      types.openIoObject[Seq[(ColId,Any)]](
        FileRef(dir, dir.id(0))).asInstanceOf[IoSeq[IoId, (ColId, Any)]]
    val ioCols =
      types.openIoObject[Seq[IoObject[IoId]]](
        FileRef(dir, dir.id(1))).asInstanceOf[IoSeq[IoId, IoSeq[IoId, Any]]]
    apply(ioIds, ioCols, df.lsize)(indexColIdOrdering)
  }


  val TypeRef(seqPkg, seqSymbol, anyArgs) = typeOf[scala.Seq[Any]]

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

/*    val constructor =
      t.getConstructors.find(_.getParameterCount == fields.size).get  // fields.map(_._1.getType).toArray :_*)

    val constructorParamNames =
      tag.tpe.members.filter(_.isConstructor).head.asMethod.paramLists.head.map(_.name.decoded.trim).toArray

    val constructorParamIndexes =
      constructorParamNames.map(e => fields.indexWhere(_._2 == e))

    val make = { vs : Array[Any] =>
      constructor.newInstance(constructorParamIndexes.map(vs(_)).asInstanceOf[Array[AnyRef]] : _*).asInstanceOf[T]
    }*/
    new TypeIoSchema[IoId, T](
      t,
      types.ioTypeOf(
        scala.reflect.runtime.universe.internal.typeRef(
          seqPkg, seqSymbol, anyArgs)),
      fields)
  }

  def openTyped[T : ClassTag](dir:Dir[IoId])(implicit tag:TypeTag[T]) : TypedDf[IoId, T] = {
    val t = typeSchema[T]
    new TypedDfView[IoId, T](
      open[String](dir))
  }

  def createTyped[T : ClassTag](items:Seq[T], dir:Dir[IoId])(
    implicit tag:TypeTag[T]) : TypedDf[IoId, T] = {
    writeTyped(items, dir)
    openTyped(dir)
  }

  def writeTyped[T : ClassTag](items:Seq[T], dir:Dir[IoId])(
    implicit tag:TypeTag[T]) = {
    val t = typeSchema[T]
    write[String](
      t.fieldIoTypes,
      t.toColumns(items),
      dir)
  }

  def openCfs(dir:Dir[IoId], id:Int) = {
    new CfsDir[IoId, IoId](dir.ref(dir.id(id)),
                           types.idSeqType,
                           types.longSeqType,
                           types.intToId _)(classTag[IoId], types.idOrdering)
  }
  def openWrittenCfs(dir:Dir[IoId], id:Int) = {
    new WrittenCfsDir[IoId, IoId](dir.ref(dir.id(id)),
                                  types.idSeqType,
                                  types.longSeqType,
                                  types.intToId _)(classTag[IoId], types.idOrdering)
  }
  def writeMergedDb[T](a:IndexedDf[IoId,T],
                       b:IndexedDf[IoId, T],
                       dir:Dir[IoId])(
    implicit t: TypeTag[Seq[(String, Any)]]) : Unit = {
    using (openWrittenCfs(dir, 0)) { d =>
      writeMerged(a.df, b.df, d)
    }
    using(openWrittenCfs(dir, 1)) { d =>
      writeMerged(a.indexDf, b.indexDf, d)(
        t, Ordering.Tuple2(Ordering[String], types.anyOrdering))
    }
  }
  def writeTypedDb[T : ClassTag](provideItems:Seq[T],
                                 dir:Dir[IoId],
                                 indexConf : IndexConf[String] = IndexConf[String]())(
                                 implicit tag:TypeTag[T]) : Unit = {
    using (openWrittenCfs(dir, 0)) { d =>
      writeTyped[T](
        provideItems,
        d)
    }
    using (openCfs(dir, 0)) { d0 =>
      using (openTyped[T](d0)) { df =>
        using(openWrittenCfs(dir, 1)) { d =>
          writeIndex[String](
            df, d, indexConf)
        }
      }
    }
  }
  def openTypedDb[T: ClassTag](dir:Dir[IoId])(
                               implicit tag:TypeTag[T]) : IndexedDf[IoId, T] = {
    using (openCfs(dir, 0)) { d =>
      val df = openTyped[T](d)
      using(openCfs(dir, 1)) { d2 =>
        new IndexedDf[IoId, T](df,
          openIndex(df, d2))
      }
    }
  }
  def createTypedDb[T:ClassTag](items : Seq[T],
                                dir:Dir[IoId],
                                indexConf : IndexConf[String] = IndexConf[String]())(
                                 implicit tag:TypeTag[T]) : IndexedDf[IoId, T] = {
    writeTypedDb(items, dir, indexConf)
    openTypedDb[T](dir)
  }
  def haveTypedDb[T : ClassTag](provideItems: => Seq[T],
                                dir:Dir[IoId],
                                indexConf : IndexConf[String] = IndexConf[String]())(
                                 implicit tag:TypeTag[T]) : IndexedDf[IoId, T] = {
    if (!dir.exists(dir.id(0)) || !dir.exists(dir.id(1))) {
      writeTypedDb(provideItems, dir, indexConf)
    }
    openTypedDb[T](dir)
  }

  def mergeTypedDbs[T : ClassTag](from:Seq[File], mergeDir:File, targetDir:File)(implicit tag:TypeTag[T]) = {
    var segments = from

    val dfs = Dfs.default
    var id = 0

    while (segments.size > 1) {
      val newSegments = new ArrayBuffer[File]()
      segments.grouped(2).foreach { g =>
        if (g.size == 2) {
          using(IoScope.open) { implicit bind =>
            val db0 = bind(dfs.openTypedDb[T](new MMapDir(g(0))))
            val db1 = bind(dfs.openTypedDb[T](new MMapDir(g(1))))
            val f = new File(mergeDir, f"_$id")
            id += 1
            dfs.writeMergedDb[T](db0, db1, new MMapDir(f))
            newSegments += f
          }
        } else {
          newSegments += g(0)
        }
      }
      segments = newSegments
    }

    segments.head.renameTo(targetDir)
  }
}
case class TypeIoSchema[IoId, T](t:Class[_],
                                 thisIoType:IoType[IoId, _ <: IoObject[IoId]],
                                 fields : Seq[(Type, String, _ <: IoType[IoId, _ <: IoObject[IoId]])]) {

  def fieldIoTypes: Seq[(String, _ <: IoType[IoId, _ <: IoObject[IoId]])] =
    Seq("this" -> thisIoType) ++ fields.map(e => (e._2, e._3)).toSeq

  def toColumns(items: Seq[T]) =
    Seq(items) ++
      fields.map { case (field, name, vt) =>
        //          System.out.println("matching '" + name + "' with " + t.getMethods.map(e => e.getName + "/" + e.getParameterCount + "/" + (e.getName == name)).mkString(","))
        val accessor =
          t.getMethods.find(m => (m.getName == name)
            && (m.getParameterCount == 0)).get
        items.map { i => accessor.invoke(i) }
      }
}