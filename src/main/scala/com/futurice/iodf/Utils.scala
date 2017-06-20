package com.futurice.iodf

import java.io.{Closeable, File}

import com.futurice.iodf.ioseq._
import com.futurice.iodf.store.{Dir, RamDir}
import xerial.larray.buffer.LBufferConfig

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Promise

/**
  * NOT thread safe: DO NOT SHARE scopes across threads!
  */
class IoScope extends Closeable{
  val closeables = ArrayBuffer[Closeable]()
  override def close(): Unit = {
    closeables.foreach(_.close)
    closeables.clear()
  }
  def bind[T <: Closeable](c:T) = {
    closeables += c
    c
  }
  def apply[T <: Closeable](c:T) : T = bind[T](c)
  def openScope = bind(new IoScope)

}

object IoScope {
  def open : IoScope = new IoScope()
}

class IoContext[IoId](openedDir:Dir[IoId]) extends Closeable {
  val dir = openedDir

  val bits =
    new IoBitsType(
      new SparseIoBitsType[IoId](),
      new DenseIoBitsType[IoId]())

  override def close(): Unit = {
    dir.close()
  }
}

object IoContext {
  def open = {
    new IoContext[Int](RamDir())
  }
  def apply()(implicit scope:IoScope) = {
    scope.bind(open)
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
