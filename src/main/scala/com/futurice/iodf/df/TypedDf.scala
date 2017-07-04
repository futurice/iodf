package com.futurice.iodf.df

import com.futurice.iodf.util.LSeq
import com.futurice.iodf.io.{IoObject, IoType}
import com.futurice.iodf.ioseq.{IoSeq, IoSeqType}

import scala.reflect.runtime.universe._
import scala.reflect.{ClassTag, classTag}

case class TypeSchema[T](t:Class[_],
                         fields : Seq[(Type, String)]) {

  def getAccessor(name:String) =
    t.getMethods.find(m => (m.getName == name) && (m.getParameterCount == 0))

  def getter(name:String) = {
    getAccessor(name).map { a =>
      (v: T) => a.invoke(v)
    }
  }

  def toColumns(items: LSeq[T]) =
  //   Seq(items) ++
    LSeq(
      fields.map { case (field, name) =>
        //          System.out.println("matching '" + name + "' with " + t.getMethods.map(e => e.getName + "/" + e.getParameterCount + "/" + (e.getName == name)).mkString(","))
        val accessor = getAccessor(name).get
        val rv = new Array[Any](items.size)
        items.zipWithIndex.foreach { case (item, i) =>
          rv(i) = accessor.invoke(item)
        }
        LSeq(rv)
      })
}

trait TypedDf[T] extends Df[String] {
  def apply(i:Long) : T

  def fieldNames : Array[String]
  def fieldIndexes : Array[Int]

  def as[E : ClassTag](implicit tag:TypeTag[E]) : TypedDf[E]

  def view(from:Long, until:Long) : TypedDf[T]
}

object TypedDf {

  val TypeRef(seqPkg, seqSymbol, anyArgs) = typeOf[scala.Seq[Any]]

  def typeSchema[T: ClassTag](implicit tag: TypeTag[T]) = {
    val t = classTag[T].runtimeClass

    val fields =
      tag.tpe.members.filter(!_.isMethod).map { e =>
        (e.typeSignature,
          e.name.decoded.trim())
      }.toArray.sortBy(_._2)

    new TypeSchema[T](
      t,
      fields)
  }

  def apply[T:ClassTag](df:Df[String])(implicit tag:TypeTag[T], ord:Ordering[String]) : TypedDf[T] = {
    new TypedDf[T] {

      override type ColType[T] = LSeq[T]

      def as[E : ClassTag](implicit tag2:TypeTag[E]) : TypedDf[E] = {
        TypedDf[E](new DfRef(df))
      }

      def view(from:Long, until:Long) = {
        TypedDf[T](df.view(from, until))
      }

      //  lazy val thisColId = indexOf("this")

      val (make, constructorParamNames, constructorParamTypes, fieldNames, fieldTypes) = {

        val fields =
          tag.tpe.members.filter(!_.isMethod).map { e =>
            (e.typeSignature,
              e.name.decoded.trim())
          }.toArray.sortBy(_._2)

        val t = classTag[T].runtimeClass
        val constructor =
          t.getConstructors.find(_.getParameterCount == fields.size).get  // fields.map(_._1.getType).toArray :_*)
        val constructorParams =
          tag.tpe.members.filter(e => e.isConstructor).head.asMethod.paramLists.head
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
        }, constructorParamNames, constructorParamTypes, fields.map(_._2), fields.map(_._1))
      }

      override def apply(i: Long): T = {
        make((0 until colIds.size).map(j => df.apply(j, i) : AnyRef).toArray)
      }

      //    df.apply[Object](thisColId, i).asInstanceOf[T]

      override def colIdOrdering = ord

      override def colIds = LSeq(fieldNames)

      override def colTypes = LSeq(fieldTypes)

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

  def apply[T:ClassTag:TypeTag](items:LSeq[T]) : TypedDf[T] = {
    val t = typeSchema[T]
    apply(
      Df[String](
        LSeq(t.fields.map(_._2)), // names
        LSeq(t.fields.map(_._1)), // types
        t.toColumns(items),
        items.size))
  }
  def apply[T:ClassTag:TypeTag](items:Seq[T]) : TypedDf[T] = {
    apply(LSeq(items))
  }
}