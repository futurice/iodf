package com.futurice.iodf.store

import java.io._

import com.futurice.iodf.{IoScope, io}
import com.futurice.iodf.util.{LSeq, Ref}
import com.futurice.iodf.Utils._
import com.futurice.iodf.io.{DataAccess, DataOutputMixin, DataRef}
import xerial.larray.buffer.{LBufferAPI, Memory}
import xerial.larray.mmap.{MMapBuffer, MMapMode}

object MMapDir {
  def apply(dir:File)(implicit scope:IoScope) = scope.bind(new MMapDir(dir))
  def open(dir:File) = new MMapDir(dir)
}

object MMapFile {
  def apply(file:File)(implicit bind:IoScope) = {
    val d = MMapDir(file.getParentFile)
    val rv = d.ref(file.getName)
    d.close
    rv
  }
  def apply(dir:File, file:String)(implicit bind:IoScope) = {
    val d = MMapDir(dir)
    val rv = d.ref(file)
    d.close
    rv
  }
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
    new io.DataOutput with DataOutputMixin {
      val out =
        new BufferedOutputStream(new FileOutputStream(file(name)))
      var pos = 0L

      override def close = out.close
      override def openDataRef: DataRef = {
        out.flush
        MMapDir.this.openRef(name)
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
      override def flush = out.flush
    }
  override def delete(name:String) =
    file(name).delete()
  override def openAccess(name: String): DataAccess = {
    val f = file(name)
    val m = new MMapBuffer(f, 0, f.length(), MMapMode.READ_ONLY)
    using (Ref.open[Memory](m.m, () => m.m.release())) { mem =>
      using (openRef(name)) { dataRef =>
        new DataAccess(dataRef, mem)
      }
    }
  }

  override def list = LSeq(dir.list.sorted)
  override def byteSize(id:String) = new File(dir, id).length()

  override def close(): Unit = {}

  def byteSize = dir.listFiles.map(_.length()).sum
}

