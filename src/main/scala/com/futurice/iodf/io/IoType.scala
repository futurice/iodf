package com.futurice.iodf.io

import java.io.{DataOutputStream, OutputStream, Writer}

import com.futurice.iodf.Utils.using
import com.futurice.iodf.store.AllocateOnce
import com.futurice.iodf.util.Ref

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

trait IoWriter[T] {
  def writingType : Type
  def write(out:DataOutput, iface:T) : Unit

  def castToWriter[To](implicit to:TypeTag[To]) : Option[IoWriter[To]] = {
    castToWriter(to.tpe).map(_.asInstanceOf[IoWriter[To]])
  }
  def castToWriter(to:Type) : Option[IoWriter[_]] = {
    to <:< writingType match {
      case true => Some(this)
      case false => None
    }
  }
  def asAnyWriter = {
    val self = this
    new IoWriter[Any] {
      override def writingType: universe.Type = self.writingType
      override def write(out: DataOutput, iface: Any): Unit =
        self.write(out, iface.asInstanceOf[T])
    }
  }
}

trait IoOpener[T] {
  def openingType : Type
  def open(ref:DataAccess) : T

  def open(dataRef: DataRef) : T = {
    using (dataRef.openAccess)(open)
  }

  def openRef(ref:DataRef) = new IoRef[T](this, ref)

  def castToOpener[To](implicit to:TypeTag[To]) : Option[IoOpener[To]] = {
    castToOpener(to.tpe).map(_.asInstanceOf[IoOpener[To]])
  }
  def castToOpener(to:Type) : Option[IoOpener[_]] = {
    openingType <:< to match {
      case true => Some(this)
      case false => None
    }
  }

}

trait IoType[Interface, IoInstance <: Interface] extends IoWriter[Interface] with IoOpener[IoInstance] {

  def interfaceType : Type
  def ioInstanceType : Type

  def writingType = interfaceType
  def openingType = ioInstanceType

  def create(ref:AllocateOnce, iface:Interface) : IoInstance = {
    using (ref.create) { out =>
      write(out, iface)
      using (out.adoptResult) { ref =>
        open(ref.openAccess)
      }
    }
  }

/*
  def as[Super >: Interface] = {
    val self = this
    new IoType[Super] {
      def valueType = self.valueType
      override def open(data: DataRef): Super =
        self.open(data)
      override def write(out: DataOutputStream, iface: Super): Unit = {
        iface match {
          case i: Interface =>
            self.write(out, i)
          case _ =>
            throw new IllegalArgumentException()
        }
      }
    }
  }*/
}

trait Merging[Interface] {
  def viewMerged(seqs:Seq[Interface]) : Interface
}

trait SizedMerging[Interface] extends Merging[Interface] {
  def defaultInstance(size:Long) : Option[Interface]
}

trait MergeableIoType[Interface, IoInstance <: Interface] extends IoType[Interface, IoInstance] with Merging[Interface] {
  def writeMerged(out:DataOutput, ss:Seq[Interface]) = {
    write(out, viewMerged(ss))
  }
}

/**
  * Makes it possible to convert LSeq[Boolean] into LBits automatically
  */
class SuperIoType[SuperIface : TypeTag, DerivedIface <: SuperIface, IoInstance <: DerivedIface](
  derivedType : IoType[DerivedIface, IoInstance],
  conversion:SuperIface=>DerivedIface)(
  implicit typ:TypeTag[SuperIface]) extends IoType[SuperIface, IoInstance] {

  override def interfaceType = typ.tpe
  override def ioInstanceType = derivedType.ioInstanceType

  override def write(out: DataOutput, iface: SuperIface): Unit =
    derivedType.write(out, conversion(iface))

  override def open(ref: DataAccess): IoInstance = {
    derivedType.open(ref)
  }
}

trait WithValueTypeTag[M] {
  def valueTypeTag : TypeTag[M]
  def valueType = valueTypeTag.tpe
}

/*
object IoTypeOf {

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
    val rv = (t1 == t2) || {
      val TypeRef(a, b, c) = t1
      val TypeRef(a2, b2, c2) = t2
      ((a == a2) || (a.toString == a2.toString)) &&
        b == b2 && !c.zip(c2).exists { case (c, c2) => !matches(c, c2) }
    }
    rv
  }
}*/
/*
abstract class IoTypeOf[Id, T <: IoObject[Id], In](implicit typ:TypeTag[In]) extends IoType[Id, T] {
  def asTypeOf[E](implicit t:Type) = {
    IoTypeOf.matches(t, typ.tpe) match {
      case true => Some(this.asInstanceOf[IoTypeOf[Id, T, E]])
      case false => None
    }
  }
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
  def apply(file:FileRef[Id], v:In)(implicit scope:IoScope) = {
    scope.bind(create(file, v))
  }
}
*/
