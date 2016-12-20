package com.futurice.iodf.store

import java.io.{Closeable, File, FileOutputStream, OutputStream}

import xerial.larray.buffer.LBufferAPI
import xerial.larray.mmap.{MMapBuffer, MMapMode}

/**
  * TODO: These should be unique, and mmaps should be unique to avoid
  *       double mmaps (!)
  */
class MMapDir(dir:File) extends Dir[String] {

  dir.mkdirs()

  def file(name:String) = new File(dir, name)

/*  override def create(name: String, length: Long): LBufferAPI = {
    new MMapBuffer(file(name), 0, length, MMapMode.READ_WRITE)
  }*/

  override def openOutput(name: String): OutputStream = {
    new FileOutputStream(file(name))
  }
  override def open(name: String, pos:Long): IoData[String] = {
    val f = file(name)
    val m = new MMapBuffer (f, 0, f.length(), MMapMode.READ_ONLY)
  //  System.out.println("memory opened")
    IoData.open(
      DataRef(this, name, pos),
      RefCounted(
        MemoryResource(m.m, new Closeable {
          def close = {
//            System.out.println("memory closed")
            m.close()
          }
        })))
  }

  override def list: Array[String] = {
    dir.list
  }

  override def close(): Unit = {}

  override def id(i:Int) : String = {
    "_" + i.toString
  }
}
