package com.futurice.iodf

import java.io.Closeable

import com.futurice.iodf.store.{FileDataRef, Dir}

case class IoRef[Id, T <: IoObject[Id]](typ:IoType[Id, _ <: T], dataRef:FileDataRef[Id]) {
  def open = typ.open(dataRef.open)
}

object IoRef {
  def apply[Id, T <: IoObject[Id]](typ:IoType[Id, T], dir:Dir[Id], id:Id, pos:Long = 0, size:Option[Long]) : IoRef[Id, T]= {
    IoRef(typ, FileDataRef(dir, id, pos, size))
  }
}

