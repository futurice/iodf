package com.futurice.iodf

import java.io.{BufferedOutputStream, DataOutputStream}

import com.futurice.iodf.Utils.using
import com.futurice.iodf.store.{FileRef, IoData}

import scala.reflect.runtime.universe._

trait IoType[Id, T <: IoObject[Id]] {
  def open(buf:IoData[Id]) : T
  def asTypeOf[E](implicit t:Type) : Option[IoTypeOf[Id, _ <: T, E]]

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
}

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

class ConvertedIoTypeOf[Id, T <: IoObject[Id], In, In2](t : IoTypeOf[Id, T, In2], conversion:In=>In2)(implicit typ:TypeTag[In])
  extends IoTypeOf[Id, T, In]()(typ) {
  override def write(output: DataOutputStream, v: In): Unit = {
    t.write(output, conversion(v))
  }
  override def open(buf: IoData[Id]): T = {
    t.open(buf)
  }
}
