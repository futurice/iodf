package com.futurice.iodf.df

import java.util

import com.futurice.iodf.IoContext
import com.futurice.iodf.df.MultiCols.DefaultColIdMemRatio
import com.futurice.iodf.util._
import com.futurice.iodf.io._
import com.futurice.iodf._
import com.futurice.iodf.ioseq.{IoSeq, SeqIoType}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._
import scala.reflect.{ClassTag, classTag}

/** FIXME: why this class is exist? */
trait ObjectsApi[T] extends LSeq[T] {
  def fieldNames   : Array[String]
  def fieldIndexes : Array[Int]
}

case class ObjectSchema[T:TypeTag:ClassTag](fields : Seq[(Type, String)]) extends ColSchema[String] {

  val sorted = fields.sortBy(_._2)

  def tTag = typeTag[T]
  def clazz = classTag[T].runtimeClass

  def getAccessor(name:String) =
    clazz.getMethods.find(m => (m.getName == name) && (m.getParameterCount == 0))

  def getter(name:String) = {
    getAccessor(name).map { a =>
      (v: T) => a.invoke(v)
    }
  }

  def toColumns(items: LSeq[T]) =
  //   Seq(items) ++
    LSeq.from(
      fields.map { case (typ, name) =>
        //          System.out.println("matching '" + name + "' with " + t.getMethods.map(e => e.getName + "/" + e.getParameterCount + "/" + (e.getName == name)).mkString(","))
        val accessor = getAccessor(name).get
        if (typ <:< typeOf[Boolean]) {
          val bitset = new util.BitSet(items.size)
          items.zipWithIndex.foreach { case (item, i) =>
            bitset.set(i.toInt, accessor.invoke(item).asInstanceOf[Boolean])
          }
          LBits.from(bitset, items.size)
        } else {
          val rv = new Array[Any](items.size)
          items.zipWithIndex.foreach { case (item, i) =>
            rv(i.toInt) = accessor.invoke(item)
          }
          LSeq.from(rv)
        }
      })

  override val colIds: LSeq[String] =
    LSeq.from(sorted.map(_._2))

  override def colTypes: LSeq[universe.Type] =
    LSeq.from(sorted.map(_._1))

  override def colMeta = LSeq.fill(fields.size, KeyMap.empty)
}

trait Objects[T] extends Df[T] with ObjectsApi[T] {

  def df : Cols[String]

  def schema : ObjectSchema[T]

  def as[E : ClassTag](implicit tag:TypeTag[E]) : Objects[E]

  override def size = lsize.toInt
  override def view(from:Long, until:Long) =
    Objects(schema, df.view(from, until))
  override def select(indexes:LSeq[Long]) =
    Objects(schema, df.select(indexes))
}

object Objects {

  def viewMerged[T:TypeTag:ClassTag](dfs:Seq[Ref[Objects[T]]],
                                     colIdMemRatio:Int = DefaultColIdMemRatio)(implicit io:IoContext) : Objects[T] = {
    Objects.apply[T](MultiCols.open[String](dfs))
  }

  val TypeRef(seqPkg, seqSymbol, anyArgs) = typeOf[scala.Seq[Any]]

  def typeSchema[T: TypeTag: ClassTag]= {
    val t = classTag[T].runtimeClass
    val tag = typeTag[T]

    val fields =
      tag.tpe.members.filter(!_.isMethod).map { e =>
        (e.typeSignature,
          e.name.decoded.trim())
      }.toArray.sortBy(_._2)

    new ObjectSchema[T](fields)
  }

  def apply[T:TypeTag:ClassTag](d:Cols[String]) : Objects[T] = {
    apply(typeSchema[T], d)
  }

  def apply[T](objectSchema:ObjectSchema[T], d:Cols[String]) : Objects[T] = {
    new Objects[T] {

      override def df = d

      override type ColType[T] = LSeq[T]

      override def schema = objectSchema

      def as[E : ClassTag](implicit tag2:TypeTag[E]) : Objects[E] = {
        Objects[E](new ColsRef(df))
      }

      val (make, constructorParamNames, constructorParamTypes) = {

        val fields =
          schema.tTag.tpe.members.filter(!_.isMethod).map { e =>
            (e.typeSignature,
              e.name.decoded.trim())
          }.toArray.sortBy(_._2)

        val t = schema.clazz
        val constructor =
          t.getConstructors.find(_.getParameterCount == fields.size).get  // fields.map(_._1.getType).toArray :_*)
        val constructorParams =
          schema.tTag.tpe.members.filter(e => e.isConstructor).head.asMethod.paramLists.head
        /*    val constructorParamTypes =
              constructorParams.map(_.typeSignature).toArray*/
        val constructorParamTypes =
          constructor.getParameterTypes
        val constructorParamNames =
          constructorParams.map(_.name.decoded.trim).toArray
        val constructorParamIndexes =
          constructorParamNames.map(e => fields.indexWhere(_._2 == e))

        /*    System.out.println(
              tag + " constructor " + ((constructorParamNames zip constructorParamTypes).map { case (a, b) => a+":"+b }.mkString(",") ))*/

        ({ vs : Array[AnyRef] =>
          constructor.newInstance(constructorParamIndexes.map(vs(_)) : _*).asInstanceOf[T]
        }, constructorParamNames, constructorParamTypes)
      }

      override def apply(i: Long): T = {
        make((0 until colIds.size).map(j => df.apply(j, i) : AnyRef).toArray)
      }

      //    df.apply[Object](thisColId, i).asInstanceOf[T]

      override def colIdOrdering = df.colIdOrdering

      override def colIds = objectSchema.colIds

      override def colTypes = objectSchema.colTypes

      override def colMeta = LSeq.fill(fieldNames.size, KeyMap.empty)

      override def fieldNames = colIds.toArray
      override def fieldIndexes = (0 until colIds.size).toArray //.filter(_ != thisColIndex)

      override def _cols =
        new LSeq[ColType[_]] {
          def apply(i :Long) = df._cols.apply(i) : ColType[_]
          def lsize = colIds.lsize
        }

      override def lsize: Long = df.lsize

      override def close(): Unit = df.close
    }
  }
  def from[T:ClassTag:TypeTag](items:LSeq[T]) : Objects[T] = {
    val t = typeSchema[T]
    apply(
      Cols[String](
        LSeq.from(t.fields.map(_._2)), // names
        LSeq.from(t.fields.map(_._1)), // types
        LSeq.fill(t.fields.size, KeyMap.empty),
        t.toColumns(items),
        items.size))
  }
  def from[T:ClassTag:TypeTag](items:Seq[T]) : Objects[T] = {
    from(LSeq.from(items))
  }
}

class ObjectsIoType[T : ClassTag:TypeTag](dfType:ColsIoType[String])(implicit io:IoContext) extends MergeableIoType[Objects[T], Objects[T]] {
  override def interfaceType: universe.Type = typeOf[Objects[T]]
  override def ioInstanceType: universe.Type = typeOf[Objects[T]]
  override def open(data: DataAccess): Objects[T] = {
    val df = dfType.open(data)
    Objects[T](df)
  }
  override def write(out: DataOutput, df: Objects[T]): Unit = {
    dfType.write(out, df)
  }
  override def viewMerged(dfs: Seq[Ref[Objects[T]]]): Objects[T] = {
    implicit val io = dfType.io
    Objects[T](MultiCols.open[String](dfs))
  }
}

