package com.futurice.iodf.store

import java.io.Closeable

import com.futurice.iodf.{IoContext, IoScope}
import com.futurice.iodf.io.{DataOutput, DataRef}
import com.futurice.iodf._

import scala.reflect.runtime.universe._

trait AllocateOnce {
  def create : DataOutput

  def save[T:TypeTag](v:T)(implicit io:IoContext) : DataRef = {
    io.save[T](this, v)
  }

  def saved[T:TypeTag](v:T)(implicit io:IoContext) : T = {
    save[T](v).as[T]
  }
}

trait Allocator extends AllocateOnce {
  def create : DataOutput
}
