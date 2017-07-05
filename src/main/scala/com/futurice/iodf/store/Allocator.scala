package com.futurice.iodf.store

import java.io.{Closeable, DataOutputStream, OutputStream}

import com.futurice.iodf.Utils.using
import com.futurice.iodf.io.{DataAccess, DataOutput, DataOutputMixin, DataRef}
import com.futurice.iodf.util._
import org.slf4j.LoggerFactory
import xerial.larray.buffer.{LBuffer, LBufferConfig, Memory}
import xerial.larray.mmap.{MMapBuffer, MMapMode}

import scala.collection.mutable
import scala.reflect.ClassTag

trait AllocateOnce extends Closeable {
  def create : DataOutput
}

trait Allocator extends AllocateOnce {
  def create : DataOutput
}
