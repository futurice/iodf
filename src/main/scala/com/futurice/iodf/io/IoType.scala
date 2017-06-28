package com.futurice.iodf.io

import java.io.DataOutputStream

import com.futurice.iodf.Utils.using
import com.futurice.iodf.store.{DataCreatorRef, DataRef}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

trait IoType[Interface] {

  def valueType : Type

  def open(data:DataRef) : Interface
  def write(out:DataOutputStream, iface:Interface) : Unit
  def create(ref:DataCreatorRef, iface:Interface) : Interface = {
    using (ref.create) { out =>
      write(new DataOutputStream(out), iface)
      open(out.adoptResult)
    }
  }

  def cast[To](implicit to:TypeTag[To]) : Option[IoType[To]] = {
    cast[To](to.tpe)
  }
  def cast[To](to:Type) : Option[IoType[To]] = {
    valueType <:< to match {
      case true => Some(this.asInstanceOf[IoType[To]])
      case false => None
    }
  }
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
  }
}

trait IoObjectType[T <: IoObject] extends IoType[T] {}

/**
  * Makes it possible to convert LSeq[Boolean] into LBits automatically
  */
class SuperIoType[SuperIface, DerivedIface <: SuperIface](
  derivedType : IoType[SuperIface],
  conversion:SuperIface=>DerivedIface)(
  implicit typ:TypeTag[SuperIface]) extends IoType[SuperIface] {

  override def valueType: universe.Type = typ.tpe

  override def open(data: DataRef): SuperIface =
    derivedType.open(data)

  override def write(out: DataOutputStream, iface: SuperIface): Unit =
    derivedType.write(out, conversion(iface))
}

/*
trait WithValueTypeTag[M] {
  def valueTypeTag : TypeTag[M]
}

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
