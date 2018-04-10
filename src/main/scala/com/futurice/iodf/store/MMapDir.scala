package com.futurice.iodf.store

import java.io._
import java.nio.file.StandardCopyOption.{ATOMIC_MOVE, REPLACE_EXISTING}
import java.nio.file._

import com.futurice.iodf.{IoScope, io}
import com.futurice.iodf.util.{LSeq, Ref}
import com.futurice.iodf.Utils._
import com.futurice.iodf._
import com.futurice.iodf.io.{DataAccess, DataOutputMixin, DataRef}
import org.slf4j.LoggerFactory
import xerial.larray.buffer.{LBufferAPI, Memory}
import xerial.larray.mmap.{MMapBuffer, MMapMode}

object MMapDir {
  def apply(dir:File)(implicit io:IoContext) = new MMapDir(dir)
  def apply(dir:String)(implicit io:IoContext) = new MMapDir(new File(dir))
}

object MMapFile {
  private val logger = LoggerFactory.getLogger(getClass)

  def apply(file:File)(implicit io:IoContext): MutableFileRef[String] = {
    val d = MMapDir(file.getParentFile)
    val rv = d.ref(file.getName)
    d.close
    rv
  }
  def apply(dir:File, file:String)(implicit io:IoContext) = {
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
class MMapDir(dir:File)(implicit io:IoContext) extends MutableDir[String] {

  val logger = LoggerFactory.getLogger(getClass)

  dir.mkdirs()

  def file(name:String) = new File(dir, name)

  def lsize = dir.list().size

  override def create(name: String) =
    new com.futurice.iodf.io.DataOutput with DataOutputMixin {
      private val parentPath: Path = dir.toPath
      val tmp =
        Files.createTempFile(parentPath, name, "tmp")

      val out =
        new BufferedOutputStream(Files.newOutputStream(tmp))
      var pos = 0L

      override def close: Unit = {
        out.close
        Utils.atomicMove(tmp, file(name).toPath)
      }

      override def dataRef: DataRef = {
        out.flush
        MMapDir.this.ref(name)
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

  override def rename(from:String, to:String) =
    file(from).renameTo(file(to))

  override def access(name: String): DataAccess = {
    val f = file(name)

    new DataAccess(
      ref(name),
      io.autoClosing.add(
        new MMapBuffer(f, 0, f.length(), MMapMode.READ_ONLY)) (_.close()))
  }

  override def list = {
   Option(dir.list) match {
     case None => throw new NoSuchFileException(s"MMapDir does not exist or is a file: ${dir}")
     case Some(a: Array[String]) => LSeq.from(a.sorted)
   }
  }
  override def byteSize(id:String) = new File(dir, id).length()

  override def close(): Unit = {}

  def byteSize = dir.listFiles.map(_.length()).sum
}

