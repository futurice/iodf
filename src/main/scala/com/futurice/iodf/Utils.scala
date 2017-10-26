package com.futurice.iodf

import java.io.{Closeable, File}

import scala.reflect.runtime.universe._
import com.futurice.iodf.io.{DataRef, IoType, IoTypes}
import com.futurice.iodf.ioseq._
import com.futurice.iodf.store._
import com.futurice.iodf.util.{LBits, LSeq, Ref}
import com.futurice.iodf.Utils._
import com.futurice.iodf.df.{ColsIoType, IndexedIoType, IndexedObjectsIoType, ObjectsIoType}
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

  var closed = false

  /** transfer ownership of all closeable resources to a new IoScope
    * TODO: think of the naming
    */
  def adopt() = {
    val rv = new IoScope()
    closeables.foreach { rv.bind(_) }
    closeables.clear()
    rv
  }
  override def close(): Unit = {
    closeables.foreach( _.close)
    closed = true
    closeables.clear()
  }
  def bind[T](v:T) = {
    if (closed) {
      throw new IllegalStateException("already closed")
    }
    v match {
      case c : Closeable =>
        closeables += c
      case _ =>
    }
    v
  }
  def unbind[T](v:T) = {
    closeables.indexOf(v) match {
      case -1 =>
      case v => closeables.remove(v)
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

  lazy val bits =
    types.ioTypeOf[LBits].asInstanceOf[BitsIoType]

  def openSave[T:TypeTag](ref:AllocateOnce, t:Ref[T]) : DataRef =
    types.openSave[T](ref, t)

  def save[T:TypeTag](ref:AllocateOnce, t:Ref[T])(implicit scope:IoScope) : DataRef =
    types.save[T](ref, t)

  def openAs[T](ref:DataRef) : Ref[T] =
    using (ref.openAccess) { data =>
      types.openAs[T](ref)
    }

  def as[T](ref:DataRef)(implicit bind:IoScope) : Ref[T] =
    bind(openAs[T](ref))

  override def close(): Unit = {
    allocatorRef.close()
  }

  def withIoType(t:IoType[_, _])(implicit bind:IoScope) : IoContext = {
    bind(new IoContext(types + t, allocatorRef))
  }

  def withType[T:TypeTag:ClassTag](implicit bind:IoScope) : IoContext = {
    implicit val io = this
    val typedDfs =
      new ObjectsIoType[T](IoTypes.stringDfType)
    val indexedDfs =
      new IndexedObjectsIoType[T](typedDfs, IoTypes.indexType)

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

  val dummyCloseable = new Closeable {
    override def close() = {}
  }

  /**
    * Returns three values, that are (match, ceil, floor)
    * @param sortedSeq
    * @param target
    * @param from
    * @param until
    * @param ord
    * @tparam T
    * @return
    */
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
