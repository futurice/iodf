package com.futurice.iodf.util

import java.io.Closeable
import java.util

import com.futurice.iodf.IoContext

import scala.collection.mutable.ArrayBuffer
import scala.ref.WeakReference

trait AutoClosed[T] {
  /* as long the lifeCycle value is reachable, the autoclosed wont't be closed*/
  def lifeCycle : Any
  def get : T
  def map[E](f: T => E) : AutoClosed[E] = {
    val t = this.lifeCycle
    val v = f(this.get)
    new AutoClosed[E] {
      def lifeCycle = t
      def get = v
    }
  }
}

object AutoClosed {
  def apply[T](v:T)(closer:T => Unit)(implicit io:IoContext) = {
    io.autoClosing.add(v)(closer)
  }
  def apply[T <: AutoCloseable](v:T)(closer:T => Unit)(implicit io:IoContext) = {
    io.autoClosing.add(v)
  }
}

trait AutoClosing extends Closeable {

  def add[T](t:T, closer:AutoCloseable) : AutoClosed[T]
  def add[T <: AutoCloseable](t:T) : AutoClosed[T] = add(t, t)
  def add[T](t: T)(closer: T => Unit): AutoClosed[T] = {
    add(t, new AutoCloseable { def close = closer(t) })
  }

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
            override def lifeCycle = this
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