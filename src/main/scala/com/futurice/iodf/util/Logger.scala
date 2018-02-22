package com.futurice.iodf.util

import com.futurice.iodf.Utils
import org.slf4j.LoggerFactory

trait Logger {
  def trace(msg:String) : Unit
  def t(msg:String) : Unit

  def debug(msg:String) : Unit
  def d(msg:String) : Unit

  def info(msg:String) : Unit
  def i(msg:String) : Unit

  def warn(msg:String) : Unit
  def w(msg:String) : Unit
  def error(msg:String) : Unit
  def e(msg:String) : Unit
  def error(msg:String, e:Throwable) : Unit
  def e(msg:String, e:Throwable) : Unit

  def iMsHeap[T](op:String)(f : => T) : T
  def dMsHeap[T](op:String)(f : => T) : T
  def tMsHeap[T](op:String)(f : => T) : T

  def iMs[T](op:String)(f : => T) : T
  def dMs[T](op:String)(f : => T) : T
  def tMs[T](op:String)(f : => T) : T


}

trait Logging {
  def apply(clazz:Class[_]) : Logger
  def apply(name:String) : Logger
}

class Slf4jLogger(l:org.slf4j.Logger) extends Logger {
  def trace(msg:String) : Unit = l.trace(msg)
  def t(msg:String) : Unit = trace(msg)

  def debug(msg:String) : Unit = l.debug(msg)
  def d(msg:String) : Unit = debug(msg)

  def info(msg:String) : Unit = l.info(msg)
  def i(msg:String) : Unit = info(msg)

  def warn(msg:String) = l.warn(msg)
  def w(msg:String) = warn(msg)
  def error(msg:String) = l.error(msg)
  def e(msg:String) = error(msg)
  def error(msg:String, e:Throwable) = l.error(msg, e)
  def e(msg:String, e:Throwable) = error(msg, e)

  private def ms[T](l : String => Unit)(op:String)(f : => T) : T = {
    val before = System.currentTimeMillis()
    l(op + " started")
    val rv =
      try {
        f
      } catch {
        case e : Throwable =>
          error(op + " failed after " + (System.currentTimeMillis - before) + " ms", e)
          e.printStackTrace()
          throw e
      }
    l(op + " took " + (System.currentTimeMillis() - before) + " ms")
    rv
  }

  def iMs[T](op: String)(f : => T): T = ms[T](i)(op)(f)
  def dMs[T](op: String)(f : => T): T = ms[T](d)(op)(f)
  def tMs[T](op: String)(f : => T): T = ms[T](t)(op)(f)
  
  def iMsHeap[T](op:String)(f : => T) = msHeap[T](i)(op)(f)
  def dMsHeap[T](op:String)(f : => T) = msHeap[T](d)(op)(f)
  def tMsHeap[T](op:String)(f : => T) = msHeap[T](t)(op)(f)

  private def msHeap[T](l: String => Unit)(op:String)(f : => T) = {
    val beforeMs = System.currentTimeMillis()
    val beforeHeap = Utils.memory
    l(op + " started with " + (beforeHeap / (1024*1024)) + " MB")
    val rv =
      try {
        f
      } catch {
        case e : Throwable =>
          error(op + " failed after " + (System.currentTimeMillis - beforeMs) + " ms", e)
          throw e
      }
    val afterHeap = Utils.memory
    l(op + " took " + (System.currentTimeMillis() - beforeMs) + " ms and " + (afterHeap / (1024*1024)) + " MB (+" +  ((beforeHeap-afterHeap) / (1024*1024)) + "MB)")
    rv
  }

}
class Slf4jLogging extends Logging {
  def apply(clazz:Class[_]) = {
    new Slf4jLogger(LoggerFactory.getLogger(clazz))
  }
  def apply(name:String) = {
    new Slf4jLogger(LoggerFactory.getLogger(name))
  }
}

object MockLogger extends Logger {
  def info(msg:String) = Unit
  def i(msg:String) = Unit

  def warn(msg:String) = Unit
  def w(msg:String) = Unit
  def error(msg:String) = Unit
  def e(msg:String) = Unit
  def error(msg:String, e:Throwable) = Unit
  def e(msg:String, e:Throwable) = Unit

  def iMs[T](op:String)(f : => T) : T = f
  def dMs[T](op:String)(f : => T) : T = f
  def tMs[T](op:String)(f : => T) : T = f
  def iMsHeap[T](op:String)(f : => T) : T = f
  def dMsHeap[T](op:String)(f : => T) : T = f
  def tMsHeap[T](op:String)(f : => T) : T = f

  def trace(msg:String) = Unit
  def t(msg:String) = Unit

  def debug(msg:String) = Unit
  def d(msg:String) = Unit

}


object MockLogging extends Logging{
  def apply(clazz:Class[_]) = MockLogger
  def apply(name:String) = MockLogger
}

object Logger {

  var logging : Logging = new Slf4jLogging()


  def setLogging(logging:Logging) = {
    this.logging = logging
  }

  def apply(clazz:Class[_]) = logging(clazz)
  def apply(name:String) = logging(name)

  def logged[T](name:String)(f:Logger => T): T = {
    val l = Logger(name)
    l.iMs(name) {f(l)}
  }

  def loggingDisabled[T](f : => T) = {
    val old =  logging
    setLogging(MockLogging)
    val rv : T = f
    setLogging(old)
    rv
  }

}
