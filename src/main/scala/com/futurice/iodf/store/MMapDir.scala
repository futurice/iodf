package com.futurice.iodf.store

import java.io._

import com.futurice.iodf.IoScope
import xerial.larray.buffer.{LBufferAPI, Memory}
import xerial.larray.mmap.{MMapBuffer, MMapMode}

object MMapDir {
  def apply(dir:File)(implicit scope:IoScope) = scope.bind(new MMapDir(dir))
}

/**
  * TODO: These should be unique, and mmaps should be unique to avoid
  *       double mmaps (!)
  */
class MMapDir(dir:File) extends MutableDir[String] {

  dir.mkdirs()

  def file(name:String) = new File(dir, name)

/*  override def create(name: String, length: Long): LBufferAPI = {
    new MMapBuffer(file(name), 0, length, MMapMode.READ_WRITE)
  }*/
  override def create(name: String) =
    new DataCreator {
      val out =
        new BufferedOutputStream(new FileOutputStream(file(name)))
      var pos = 0L

      override def close = out.close
      override def openDataRef: DataRef = {
        MMapDir.this.openRef(name)
      }
      override def adoptResult: DataRef = {
        close
        openRef(name)
      }
      override def write(b: Int): Unit = {
        out.write(b)
        pos += 1
      }
      override def write(b: Array[Byte]): Unit = {
        out.write(b)
        pos += b.length
      }
      override def write(b: Array[Byte], off:Int, len:Int): Unit = {
        out.write(b, off, len)
        pos += len
      }
    }
  override def delete(name:String) =
    file(name).delete()
  override def open(name: String): RandomAccess = {
    val f = file(name)
    val m = new MMapBuffer(f, 0, f.length(), MMapMode.READ_ONLY)
    new RandomAccess(Ref[Memory](m.m, () => m.m.release()))
  }

  override def list: Array[String] = {
    dir.list
  }
  override def byteSize(id:String) = new File(dir, id).length()

  override def close(): Unit = {}

  override def id(i:Int) : String = {
    "_" + i.toString
  }
  def byteSize = dir.listFiles.map(_.length()).sum
}
