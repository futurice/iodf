package com.futurice.iodf.store

import java.io.{Closeable, InputStream, OutputStream}
import java.util.logging.{Level, Logger}

import com.futurice.iodf.IoScope
import com.futurice.iodf.Utils._

import scala.collection.mutable

// TODO: replace dir with land, and file with resource.
//       requiring all memory areas to be located in directory, messes up the cleaning



class FileRef[Id](val dir:Dir[Id], val id:Id) extends DataRef {
  def close = {}
  def byteSize = dir.byteSize(id)
  def open : RandomAccess = dir.open(id)
  def openView(from:Long, until:Long) =
    new DataRefView(this, from, until)
  def copy = new FileRef(dir, id)
}

class WritableFileRef[Id](dir:WritableDir[Id], id:Id) extends FileRef(dir, id) with AllocateOnce {
  def create = dir.create(id)
  override def copy = new WritableFileRef[Id](dir, id)
}

class MutableFileRef[Id](dir:MutableDir[Id], id:Id) extends WritableFileRef(dir, id) {
  def delete = dir.delete(id)
  override def copy = new MutableFileRef[Id](dir, id)
}

trait Dir[Id] extends Closeable {

  // select i:th id in order
  def id(i:Int) : Id

  def list : Array[Id]
  def exists(id:Id) = list.contains(id)
  def byteSize(id:Id) : Long

  def openRef(id:Id) =
    new FileRef(this, id)
/*  def openRefInt(i:Int) =
    openRef(id(i))*/

  def ref(id:Id)(implicit bind:IoScope) = bind(openRef(id))
  //def refInt(i:Int)(implicit bind:IoScope) = bind(openRefInt(i))

  def open(id:Id) : RandomAccess

  def byteSize : Long
}

trait WritableDir[Id] extends Dir[Id] {
  def freeId : Id = {
    def find(i:Int) : Id = {
      val rv = id(i)
      if (exists(rv)) {
        find(i+1)
      } else {
        rv
      }
    }
    find(0)
  }
  def freeFileRef = openRef(freeId)

  override def openRef(id:Id) =
    new WritableFileRef[Id](this, id)
  override def ref(id:Id)(implicit bind:IoScope) : WritableFileRef[Id] =
    bind(openRef(id))

  def create(id:Id) : DataCreator

}

trait MutableDir[Id] extends WritableDir[Id] {
  def delete(id:Id) : Boolean
  override def openRef(id:Id) =
    new MutableFileRef[Id](this, id)
}

