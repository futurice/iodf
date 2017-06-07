package com.futurice.iodf

import java.io._
import java.util

import com.futurice.iodf.store._
import com.futurice.iodf.ioseq._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import scala.reflect._
import com.futurice.iodf.Utils._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ArrayBuffer


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
  def colType[T](i:Long) : SeqIoType[IoId, _ <: IoSeq[IoId, T], T] = {
    using (openCol(i)) { _.ref.typ.asInstanceOf[SeqIoType[IoId,_ <: IoSeq[IoId, T],T]] }
  }
  def colType[T](id:ColId) : SeqIoType[IoId, _ <: IoSeq[IoId, T], T] = {
    colType(indexOf(id))
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

case class TypeIoSchema[IoId, T](t:Class[_],
                                 thisIoType:IoType[IoId, _ <: IoObject[IoId]],
                                 fields : Seq[(Type, String, _ <: IoType[IoId, _ <: IoObject[IoId]])]) {

  def fieldIoTypes: Seq[(String, _ <: IoType[IoId, _ <: IoObject[IoId]])] =
//    Seq("this" -> thisIoType) ++
      fields.map(e => (e._2, e._3)).toSeq

  def getAccessor(name:String) =
    t.getMethods.find(m => (m.getName == name) && (m.getParameterCount == 0))

  def getter(name:String) = {
    getAccessor(name).map { a =>
      (v: T) => a.invoke(v)
    }
  }

  def toColumns(items: Seq[T]) =
 //   Seq(items) ++
      fields.map { case (field, name, vt) =>
        //          System.out.println("matching '" + name + "' with " + t.getMethods.map(e => e.getName + "/" + e.getParameterCount + "/" + (e.getName == name)).mkString(","))
        val accessor = getAccessor(name).get
        items.map { i => accessor.invoke(i) }
      }
}