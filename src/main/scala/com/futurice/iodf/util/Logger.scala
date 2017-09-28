package com.futurice.iodf.util

import com.futurice.iodf.Utils
import org.slf4j.LoggerFactory

class Logger(l:org.slf4j.Logger) {
  def info(msg:String) = l.info(msg)
  def i(msg:String) = info(msg)

  def error(msg:String) = l.error(msg)
  def e(msg:String) = error(msg)
  def error(msg:String, e:Throwable) = l.error(msg, e)
  def e(msg:String, e:Throwable) = error(msg, e)

  def iMs[T](op:String, f : => T) = {
    val before = System.currentTimeMillis()
    i(op + " started")
    val rv =
      try {
        f
      } catch {
        case e : Throwable =>
          error(op + " failed after " + (System.currentTimeMillis - before) + " ms", e)
          e.printStackTrace()
          throw e
      }
    i(op + " took " + (System.currentTimeMillis() - before) + " ms")
    rv
  }

  def iMsHeap[T](op:String, f : => T) = {
    val beforeMs = System.currentTimeMillis()
    val beforeHeap = Utils.memory
    i(op + " started with " + (beforeHeap / (1024*1024)) + " MB")
    val rv =
      try {
        f
      } catch {
        case e : Throwable =>
          error(op + " failed after " + (System.currentTimeMillis - beforeMs) + " ms", e)
          throw e
      }
    val afterHeap = Utils.memory
    i(op + " took " + (System.currentTimeMillis() - beforeMs) + " ms and " + (afterHeap / (1024*1024)) + " MB (+" +  ((beforeHeap-afterHeap) / (1024*1024)) + "MB)")
    rv
  }

}

object Logger {
  def apply(clazz:Class[_]) = {
    new Logger(LoggerFactory.getLogger(clazz))
  }
  def apply(name:String) = {
    new Logger(LoggerFactory.getLogger(name))
  }
}