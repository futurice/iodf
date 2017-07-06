package com.futurice.iodf

import java.io.{Closeable, File}

import scala.reflect.runtime.universe._
import com.futurice.iodf.io.{DataRef, IoType, IoTypes}
import com.futurice.iodf.ioseq._
import com.futurice.iodf.store._
import com.futurice.iodf.util.{LBits, LSeq, Ref}
import com.futurice.iodf.Utils._
import com.futurice.iodf.df.{DfIoType, IndexedDfIoType, TypedDfIoType}
import xerial.larray.buffer.LBufferConfig

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Promise
import scala.reflect.ClassTag

/**
  * NOT thread safe: DO NOT SHARE scopes across threads!
  */
class IoScope extends Closeable{
  val closeables = ArrayBuffer[Closeable]()
  override def close(): Unit = {
    closeables.foreach(_.close)
    closeables.clear()
  }
  def bind[T](v:T) = {
    v match {
      case c : Closeable =>
        closeables += c
      case None =>
    }
    v
  }
  def cleanup(closer:  => Unit) = {
    closeables += new Closeable {
      def close = closer
    }
    Unit
  }
  def apply[T](c:T) : T = bind[T](c)
  def openScope = bind(new IoScope)

}

object IoScope {
  def open : IoScope = new IoScope()
}

class IoContext(val types:IoTypes, _allocator:Ref[Allocator]) extends Closeable {

  val allocatorRef = _allocator.openCopy
  val allocator = allocatorRef.get

  val bits =
    types.ioTypeOf[LBits].asInstanceOf[BitsIoType]

  def openSave[T:TypeTag](ref:AllocateOnce, t:T) : DataRef =
    types.openSave[T](ref, t)

  def save[T:TypeTag](ref:AllocateOnce, t:T)(implicit scope:IoScope) : DataRef =
    types.save[T](ref, t)

  def openAs[T](ref:DataRef) : T =
    using (ref.openAccess) { data =>
      types.openAs[T](ref)
    }

  def as[T](ref:DataRef)(implicit bind:IoScope) : T =
    bind(openAs[T](ref))

  override def close(): Unit = {
    allocatorRef.close()
  }

  def withIoType(t:IoType[_, _])(implicit bind:IoScope) : IoContext = {
    bind(new IoContext(types + t, allocatorRef))
  }

  def withType[T:TypeTag:ClassTag](implicit bind:IoScope) : IoContext = {
    implicit val io = this
    val typedDfs = new TypedDfIoType[T](IoTypes.stringDfType)
    val indexedDfs = new IndexedDfIoType[T](typedDfs, IoTypes.indexDfType)

    withIoType(typedDfs).withIoType(indexedDfs)
  }

}

object IoContext {
  def apply()(implicit bind:IoScope) = {
    bind(new IoContext(IoTypes.default, bind(Ref.open(new RamAllocator()))))
  }
}

/**
  * Created by arau on 24.11.2016.
  */
object Utils {

  def binarySearch[T](sortedSeq:LSeq[T], target:T, from:Long = 0, until:Long = Long.MaxValue)(implicit ord:Ordering[T]) : (Long, Long, Long)= {
    @tailrec
    def recursion(low:Long, high:Long):(Long, Long, Long) = (low+high)/2 match{
      case _ if high < low => (-1, high, low)
      case mid if ord.gt(sortedSeq(mid), target) => recursion(low, mid-1)
      case mid if ord.lt(sortedSeq(mid), target) => recursion(mid+1, high)
      case mid => (mid, mid, mid)
    }
    recursion(from, Math.min(until, sortedSeq.lsize) - 1)
  }

  /*
  def weightedBinarySearch[T](sortedSeq:IoSeq[_, T], target:T, from:Long = 0, until:Long = Long.MaxValue, p:Double = 0.5)(implicit ord:Ordering[T]) : (Long, Long, Long)= {
    @tailrec
    def recursion(low:Long, high:Long):(Long, Long, Long) = (low+(high-low)*p).toLong match {
      case _ if high < low => (-1, low, high)
      case mid if ord.gt(sortedSeq(mid), target) => recursion(low, mid-1)
      case mid if ord.lt(sortedSeq(mid), target) => recursion(mid+1, high)
      case mid => (mid, mid, mid)
    }
    recursion(from, Math.min(until, sortedSeq.lsize) - 1)
  }*/

  def atomicWrite(file:File)(writer:File => Unit): Unit = {
    val tmpFile = new File(file.getParentFile, file.getName + ".tmp")
    try {
      writer(tmpFile)
      tmpFile.renameTo(file)
    } finally {
      tmpFile.delete()
    }
  }

  def using[T <: AutoCloseable, E](d:T)(f : T => E) = {
    try {
      f(d)
    } finally {
      d.close
    }
  }
  def scoped[E](f : IoScope => E) = {
    using(IoScope.open) { scope => f(scope) }
  }

  def offHeapMemory = {
    LBufferConfig.allocator.allocatedSize()
  }

  def memory = {
    System.gc();
    val rt = Runtime.getRuntime();
    (rt.totalMemory() - rt.freeMemory())
  }

  def fds = {
    new File("/proc/self/fd").listFiles().length
  }
}

case class SamplingStats(samples:Long, sum:Long, min:Long, max:Long) {
  def mean = sum / samples.toDouble
}

class MemoryMonitor(samplingFreqMillis:Long) extends Closeable {

  @volatile
  private var continue = true

  private val promise = Promise[SamplingStats]()

  val thread = new Thread(new Runnable() {
    def run = {
      try {
        var (n, sum, min, max) = (0L, 0L, Long.MaxValue, Long.MinValue)
        while (continue) {
          Thread.sleep(samplingFreqMillis)
          val m = Utils.memory
          n += 1
          sum += m
          min = Math.min(min, m)
          max = Math.max(max, m)
        }
        promise.success(SamplingStats(n, sum, min, max))
      } catch {
        case e : Throwable => promise.failure(e)
      }
    }
  })

  thread.start()

  def close = {
    continue = false
  }
  def finish = {
    continue = false
    promise.future
  }
}
