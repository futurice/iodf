package com.futurice.iodf.store

import java.io.{Closeable, File, FileOutputStream, OutputStream}

import com.futurice.iodf.IoScope
import xerial.larray.buffer.LBufferAPI
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
      val out = new FileOutputStream(file(name))
      override def close = out.close
      override def adoptResult: DataRef = {
        close
        openRef(name)
      }
      override def write(b: Int): Unit = out.write(b)
    }
  override def open(name: String, pos:Long, size:Option[Long]): RandomAccess = {
    val f = file(name)
    val m = new MMapBuffer(f, 0, f.length(), MMapMode.READ_ONLY)
    new RandomAccess(
      RefCounted(
        MemoryResource(m.m, new Closeable {
          def close = {
            m.close()
          }
        }), 0))
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
