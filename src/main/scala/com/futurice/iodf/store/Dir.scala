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

class WritableFileRef[Id](dir:WritableDir[Id], id:Id) extends FileRef(dir, id) with DataCreatorRef {
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

  def ref(id:Id)(implicit bind:IoScope) = bind(openRef(id))

  def open(id:Id, pos:Long = 0, size:Option[Long] = None) :
    RandomAccess

  def byteSize : Long
}

trait WritableDir[Id] extends Dir[Id] with DataLand {
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
  override def openRef(id:Id) =
    new WritableFileRef[Id](this, id)
  def freeFileRef = openRef(freeId)

  def create(id:Id) : DataCreator
  def create = create(freeId)

}

trait MutableDir[Id] extends WritableDir[Id] {
  def delete(id:Id) : Boolean
  override def openRef(id:Id) =
    new MutableFileRef[Id](this, id)
}

