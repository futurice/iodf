package com.futurice.iodf.io

import java.io.{DataOutputStream, OutputStream, Writer}

import com.futurice.iodf.IoScope
import com.futurice.iodf.Utils.using
import com.futurice.iodf.store.AllocateOnce
import com.futurice.iodf.util.Ref

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

trait TypeSupportProvider {
  def support(supported:Type, support:Type) : Any
  def supportAs[Supported:TypeTag,Support:TypeTag] = {
    support(typeOf[Supported], typeOf[Support]).asInstanceOf[Support]
  }
}

trait IoWriterProvider {
  def provideWriter(to: Type): Option[IoWriter[_]]
  def castToWriter[To](implicit to:TypeTag[To]) : Option[IoWriter[To]] = {
    provideWriter(to.tpe).map(_.asInstanceOf[IoWriter[To]])
  }
}

trait IoOpenerProvider {
  def provideOpener(to: Type): Option[IoOpener[_]]
  def castToOpener[To](implicit to:TypeTag[To]) : Option[IoOpener[To]] = {
    provideOpener(to.tpe).map(_.asInstanceOf[IoOpener[To]])
  }
}

trait TypeIoProvider extends IoWriterProvider with IoOpenerProvider {
  def +(second:TypeIoProvider) = {
    val first = this
    new TypeIoProvider {
      override def provideWriter(to: universe.Type): Option[IoWriter[_]] =
        first.provideWriter(to).orElse(second.provideWriter(to))
      override def provideOpener(to: universe.Type): Option[IoOpener[_]] =
        first.provideOpener(to).orElse(second.provideOpener(to))
    }
  }
}

object TypeIoProvider {
  def apply(first:TypeIoProvider, second:TypeIoProvider) = {

  }
}


trait IoWriter[T] extends IoWriterProvider {
  def write(out: DataOutput, iface: T): Unit

  def openSave(alloc:AllocateOnce, iface:T) = {
    using (alloc.create) { out =>
      write(out, iface)
      out.openDataRef
    }
  }
  def save(alloc:AllocateOnce, iface:T)(implicit bind:IoScope) = {
    bind(openSave(alloc, iface))
  }

  def writingType : Type

  def provideWriter(to:Type) : Option[IoWriter[_]] = {
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

trait IoOpener[T] extends IoOpenerProvider {
  def open(ref:DataAccess) : T

  def open(dataRef: DataRef) : T = {
    using (dataRef.openAccess)(open)
  }

  def openingType : Type

  def openRef(ref:DataRef) = new IoRef[T](this, ref)

  def provideOpener(to:Type) : Option[IoOpener[_]] = {
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

  def openCreated(ref:AllocateOnce, iface:Interface) : IoInstance = {
    using (openSave(ref, iface)) { data =>
      using (data.openAccess) { open }
    }
  }

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
