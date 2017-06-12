package com.futurice.iodf

import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.universe._


trait TypedDf[IoId, T] extends Df[IoId, String] {
  def apply(i:Long) : T
//  def thisColIndex : Int
  def fieldNames : Iterable[String]
  def fieldIndexes : Iterable[Int]
}

class TypedDfView[IoId, T : ClassTag](df:Df[IoId, String])(
  implicit tag:TypeTag[T], ord:Ordering[String])
  extends TypedDf[IoId, T] {

  override type ColType[T] = LSeq[T]

//  lazy val thisColId = indexOf("this")

  val (make, constructorParamNames, constructorParamTypes) = {

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
    }, constructorParamNames, constructorParamTypes)
  }

  override def apply(i: Long): T = {
    make((0 until colIds.size).map(j => df.apply(j, i) : AnyRef).toArray)
  }

//    df.apply[Object](thisColId, i).asInstanceOf[T]

  override def colIdOrdering = ord

  override def colIds = df.colIds

//  lazy val thisColIndex = indexOf("this")
  override def fieldNames = colIds // .filter(_ != "this")
  override def fieldIndexes = (0 until colIds.size) //.filter(_ != thisColIndex)

  override def _cols =
    df._cols.map { e : LSeq[Any] => e }

  override def lsize: Long = df.lsize

  override def close(): Unit = df.close
}

