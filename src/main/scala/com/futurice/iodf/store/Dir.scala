package com.futurice.iodf.store

import java.io.{Closeable, InputStream, OutputStream}
import java.util.logging.{Level, Logger}

import com.futurice.iodf.{IoScope, Utils}
import com.futurice.iodf.Utils._
import com.futurice.iodf.io.{DataAccess, DataOutput, DataRef, DataRefView}
import com.futurice.iodf.util.LSeq

import scala.collection.mutable

// TODO: replace dir with land, and file with resource.
//       requiring all memory areas to be located in directory, messes up the cleaning



class FileRef[Id](val dir:Dir[Id], val id:Id) extends DataRef {
  def close = {}
  def byteSize = dir.byteSize(id)
  def openAccess : DataAccess = dir.openAccess(id)
  def openView(from:Long, until:Long) =
    new DataRefView(this, from, until)
  def openCopy = new FileRef(dir, id)
}

class WritableFileRef[Id](dir:WritableDir[Id], id:Id) extends FileRef(dir, id) with AllocateOnce {
  def create = dir.create(id)
  override def openCopy = new WritableFileRef[Id](dir, id)
}

class MutableFileRef[Id](dir:MutableDir[Id], id:Id) extends WritableFileRef(dir, id) {
  def delete = dir.delete(id)
  override def openCopy = new MutableFileRef[Id](dir, id)
}

trait Dir[Id] extends Closeable {

  /* this list must be sorted */
  def list : LSeq[Id]
  def byteSize(id:Id) : Long

  def exists(id:Id)(implicit ord:Ordering[Id]) = Utils.binarySearch(list, id)._1 != -1

  def openRef(id:Id) = new FileRef(this, id)
  def ref(id:Id)(implicit bind:IoScope) = bind(openRef(id))

  def openAccess(id:Id) : DataAccess
  def access(id:Id)(implicit bind:IoScope) = {
    bind(openAccess(id))
  }

  def byteSize : Long
}

trait IndexReferableDir[Id] extends Dir[Id] {
  def indexOf(id:Id)(implicit ord:Ordering[Id]) = binarySearch[Id](list, id)._1

  def indexRef(index:Long) : DataRef
  def indexByteSize(index:Long) : Long
  def openIndex(index:Long) : DataAccess
}

trait WritableDir[Id] extends Dir[Id] {
  override def openRef(id:Id) =
    new WritableFileRef[Id](this, id)
  override def ref(id:Id)(implicit bind:IoScope) : WritableFileRef[Id] =
    bind(openRef(id))

  def create(id:Id) : DataOutput

}

trait MutableDir[Id] extends WritableDir[Id] {
  def delete(id:Id) : Boolean
  override def openRef(id:Id) =
    new MutableFileRef[Id](this, id)
}

