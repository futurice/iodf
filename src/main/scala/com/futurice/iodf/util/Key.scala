package com.futurice.iodf.util

import com.futurice.iodf.IoScope
import com.futurice.iodf.io.{DataAccess, DataOutput, IoType, MergeableIoType}

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe._

/**
  * TODO:  rethink Key,
  *
  *   Should Key be trait instead of case class?
  *   Also: should key be of form Key[Id, Type] instead of Key[Type]
  *   This would make the heterogenous maps more generic, and it would
  *   allow greater variation with keys, e.g..
  *
  *      ints/longs for random lookups & efficiency
  *      Paths for sequential lookups through a hierachy of environments
  */
case class Key[Type](name:String) {
  def ->(value:Type) = {
    new KeyValue[Type](this, value)
  }

  // compare both the name and the type tag
  override def equals(obj: scala.Any): Boolean = {
    obj match {
      case k : Key[_]
        if k.name == name =>
        true
      case _ =>
        false
    }
  }
}

case class KeyValue[Type](key:Key[Type], value:Type) {
  def get[Type2](key2:Key[Type2]) : Option[Type2] = {
    key2 match {
      case k if k == key => Some(value.asInstanceOf[Type2])
      case _ => None
    }
  }
}

trait KeyMap extends Iterable[KeyValue[_]] {
  def get[Type](key:Key[Type]) : Option[Type]
  def apply[Type](key:Key[Type]) = get(key).get
  def keys : Iterable[Key[_]]
  def size : Int
}

object KeyMap {
  val empty = new KeyMap {
    override def get[Type](key: Key[Type]) = None
    override def keys: Set[Key[_]] = Set.empty[Key[_]]
    override def size = 0
    override def iterator = Iterator.empty
  }
  def apply(keyValues:KeyValue[_]*) = new KeyMap {
    // FIXME: O(N) complexity
    override def get[Type](key: Key[Type]) : Option[Type] =
      keyValues.view.map(_.get(key)).collectFirst { case Some(s) => s }
    override def keys =
      keyValues.map(_.key)
    override def size = keyValues.size
    override def iterator = keyValues.iterator
  }
}

trait Resources {
  def open[Type](key:Key[Type]) : Ref[Type]
  def apply[Type](key:Key[Type])(implicit bind:IoScope) : Ref[Type] = {
    bind(open[Type](key))
  }
  def keys : Set[Key[_]]
}