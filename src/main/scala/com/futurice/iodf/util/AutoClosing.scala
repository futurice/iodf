package com.futurice.iodf.util

import java.io.Closeable
import java.util

import scala.collection.mutable.ArrayBuffer
import scala.ref.WeakReference

trait AutoClosed[T] {
  def get : T
}

trait AutoClosing extends Closeable {

  def add[T](t:T, closer:AutoCloseable) : AutoClosed[T]
  def add[T <: AutoCloseable](t:T) : AutoClosed[T] = add(t, t)

  def clean : Unit
  def openCount : Int
}

object AutoClosing {

  def apply() = {
    new AutoClosing {

      val l = Logger("AutoClosing")

      var closeables = List[(WeakReference[AutoClosed[_]], AutoCloseable)]()

      override def add[T](t: T, closer: AutoCloseable): AutoClosed[T] = {
        val rv =
          new AutoClosed[T] {
            override def get: T = t
          }
        closeables.synchronized {
          closeables = ((WeakReference.apply(rv), closer)) :: closeables
        }
        rv
      }

      override def close(): Unit = {
        System.gc()
        clean()
        closeables.synchronized {
          closeables.foreach { c =>
            l.warn(c._1 + " was still reachable, when AutoClosing was closed")
            c._2.close()
          }
        }
      }

      override def clean() : Unit = closeables.synchronized {
        closeables = closeables.flatMap { entry =>
          entry._1.get match {
            case Some(_) => Some(entry)
            case None =>
              entry._2.close
              None
          }
        }
      }

      override def openCount() : Int = closeables.synchronized {
        closeables.size
      }
    }

  }

}