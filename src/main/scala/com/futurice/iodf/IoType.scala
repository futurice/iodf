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
  def apply(file:FileRef[Id], v:In)(implicit scope:IoScope) = {
    scope.bind(create(file, v))
  }
}
