package com.futurice.iodf.store

import java.io.Closeable

import com.futurice.iodf.{IoContext, IoScope}
import com.futurice.iodf.io.{DataOutput, DataRef}
import com.futurice.iodf.Utils._

import scala.reflect.runtime.universe._

trait AllocateOnce extends Closeable {
  def create : DataOutput

  // Should this be moved to external monad?
  def save[T:TypeTag](v:T)(implicit io:IoContext, scope:IoScope) : DataRef = {
    io.save[T](this, v)
  }
  def openSave[T:TypeTag](v:T)(implicit io:IoContext) : DataRef = {
    io.openSave[T](this, v)
  }

  def openSaved[T:TypeTag](v:T)(implicit io:IoContext) : T = {
    using (openSave[T](v)) { _.openAs[T] }
  }
  def saved[T:TypeTag](v:T)(implicit io:IoContext,bind:IoScope) : T = {
    bind(openSaved[T](v))
  }
}

trait Allocator extends AllocateOnce {
  def create : DataOutput
}
