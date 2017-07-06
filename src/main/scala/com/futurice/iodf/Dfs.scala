package com.futurice.iodf

import java.io.{DataOutputStream, File, FileOutputStream}

import com.futurice.iodf.Utils._
import com.futurice.iodf.df._
import com.futurice.iodf.io._
import com.futurice.iodf.ioseq.{IoSeq, SeqIoType}
import com.futurice.iodf.util._
import com.futurice.iodf.store._
import org.slf4j.LoggerFactory
import scala.reflect.runtime.universe._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.runtime.universe
import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.universe._


/**
  * TODO: create IoType entries for various data frames
  *
  *       Make it so that you create and merge dataframes by merely
  *       calling the iotypes, e.g.
  *
  *       io.write(out, IndexedTypedDf(events))
  *
  *       val df =
  *         MultiDf(
  *           dir.listRefs.map { io.open[IndexedTypeDf[Event]] _ }
  *
  */
/*
class Dfs(val types:IoTypes) {

  val l = LoggerFactory.getLogger(this.getClass)


  def writeDf[ColId, FileId](df:Df[ColId],
                             dir: WritableDir[FileId])(
                             implicit colIdSeqTag: TypeTag[LSeq[ColId]]) : Unit = {
    l.info("writing column ids...")
    val before2 = System.currentTimeMillis()
    using (dir.openRef(dir.id(0))) { ref =>
      types.written(ref, df.colIds).close
    }
    l.info("column ids written in " + (System.currentTimeMillis() - before2) + " ms")
    l.info("writing columns...")
    val before = System.currentTimeMillis()
    val colTypeIter = df.colTypes.iterator
    df._cols.iterator.zipWithIndex.map { case (openedCol, index) =>
      using (openedCol) { col => // this opens the column
        if ((index % 10000) == 0 && index > 0)
          l.info("  written " + index + " columns in " + (System.currentTimeMillis() - before) + " ms")

        using(new DataOutputStream(dir.create(dir.id(index+1)))) { out =>
          types.write(out, col, types.toLSeqType(colTypeIter.next))
        }
      }
    }.toArray
    l.info("columns written in " + (System.currentTimeMillis() - before) + " ms")
  }

  def writeMergedDf[ColId, FileId](
    dfs: Seq[Df[ColId]], dir: WritableDir[FileId], colTypes:DfMerging[ColId])(
    implicit colIdSeqTag: TypeTag[LSeq[ColId]],
    ordering: Ordering[ColId]): Unit = {
    using (MultiDf.apply[ColId](dfs, colTypes)) { df =>
      writeDf(df, dir)
    }
  }


  def write[T:TypeTag](out: DataOutput, v:T) = {
    types.write[T](out, v)
  }

  def open[T:TypeTag](ref:DataRef) : T = {
    types.openAs[T](ref)
  }
/*
  def createDf[ColId : TypeTag, FileId](df : Df[ColId],
                              dir: WritableDir[FileId])(
                       implicit colIdSeqTag: TypeTag[LSeq[ColId]],
                       colIdOrdering: Ordering[ColId]): Df[ColId] = {
    writeDf[ColId, FileId](df, dir)
    openDf[ColId, FileId](dir)(colIdSeqTag, colIdOrdering)
  }


  def openDf[ColId, FileId](dir: Dir[FileId])(implicit colIdSeqTag: TypeTag[LSeq[ColId]],
                                              colIdOrdering: Ordering[ColId])
  : Df[ColId] = scoped { implicit scope =>
    val ioIds =
      types.openAs[LSeq[ColId]](
        dir.ref(dir.id(0)))
    val ioCols =
      new LSeq[LSeq[_]] {
        override def apply(l: Long): LSeq[_] =
          types.openAs[LSeq[_]](dir.ref(dir.id((l + 1).toInt)))
        override def lsize: Long = ioIds.lsize
      }
    val sz = ioCols.size match {
      case 0 => 0
      case _ => using(ioCols(0)) { _.lsize }
    }
    Df[ColId](ioIds,
              new LSeq[Type] {
                def lsize = ioCols.lsize
                def apply(i:Long) = // hackish!
                  ioCols(i).asInstanceOf[IoSeq[ColId]].ioSeqType.valueType
              },
              ioCols,
              sz)
  }
*/
  def indexColIdOrdering[ColId](implicit colOrd: Ordering[ColId]) = {
    new Ordering[(ColId, Any)] {
      val anyOrdering = types.anyOrdering

      override def compare(x: (ColId, Any), y: (ColId, Any)): Int = {
        colOrd.compare(x._1, y._1) match {
          case 0 => // fields matches, so the values should be of the same type
            anyOrdering.compare(x._2, y._2)
/*            x._2 match {
              case v: Boolean => v.compare(y._2.asInstanceOf[Boolean])
              case v: Int => v.compare(y._2.asInstanceOf[Int])
              case v: Long => v.compare(y._2.asInstanceOf[Long])
              case v: String => v.compare(y._2.asInstanceOf[String])
            }*/
          case v => v
        }
      }
    }
  }

  def createIndex[ColId, FileId](
          df: Df[ColId],
          dir: WritableDir[FileId],
          conf: IndexConf[ColId] = IndexConf[ColId]())
          (implicit colIdSeqTag: TypeTag[LSeq[(ColId, Any)]],
           boolSeqTag: TypeTag[LSeq[Boolean]],
           ord: Ordering[ColId]) = {
    writeIndex(df, dir, conf)
    openIndex(df, dir)
  }

  def writeIndex[ColId, FileId](
                     df: Df[ColId],
                     dir: WritableDir[FileId],
                     conf: IndexConf[ColId] = IndexConf[ColId]())
                     (implicit colIdSeqTag: TypeTag[LSeq[(ColId, Any)]],
                      boolSeqTag: TypeTag[LSeq[Boolean]],
                      ord: Ordering[ColId]) = {
    val ids = ArrayBuffer[(ColId, Any)]()
    for (i <- (0 until df.colIds.size)) {
      val id = df.colIds(i)
      if (conf.isAnalyzed(id))
        using(df.openCol[Any](i)) { col =>
          val ordering = types.orderingOf(df.colTypes(i))
          val analyzer = conf.analyzer(id)
          val distinct = col.toArray.flatMap(analyzer(_)).distinct.sorted(ordering)
          l.info(f"${distinct.size} distinct for column $id")

          val indexes = {
            val rv = Array.fill(distinct.size)(new ArrayBuffer[Long]())
            val toIndex = distinct.zipWithIndex.toMap
            for (i <- (0L until col.lsize)) {
              analyzer(col(i)).foreach { token =>
                rv(toIndex(token)) += i
              }
            }
            rv
          }

          for (index <- 0 until distinct.size) {
            val value = distinct(index)
            ids += (id -> value)
            val data = indexes(index)
            val before = System.currentTimeMillis()

            scoped { implicit bind =>
              types.written(
                dir.ref(dir.id(ids.size)),
                LBits(data, df.lsize)).close
            }
          }
        }
    }
    scoped { implicit bind =>
      types.written(
        dir.ref(dir.id(0)),
        LSeq(ids)).close
    }
  }

  def openIndex[ColId, FileId](df: Df[ColId], dir: Dir[FileId])
                               (implicit colIdSeqTag: TypeTag[LSeq[(ColId, Any)]],
                               boolSeqTag: TypeTag[LSeq[Boolean]],
                               ord: Ordering[ColId]) = {
    scoped { implicit bind =>
      val ioIds =
        types.openAs[LSeq[(ColId, Any)]](
          bind(dir.openRef(dir.id(0))))
      Df(ioIds,
         new LSeq[Type] {
           override def apply(l: Long): universe.Type = typeOf[Boolean]
           override def lsize: Long = ioIds.lsize
         },
        new LSeq[LSeq[_]] {
          override def apply(l: Long): LSeq[_] =
            using (dir.openRef(dir.id(l.toInt+1))) { ref =>
              types.openAs[LSeq[_]](ref)
            }
          override def lsize: Long = ioIds.lsize
        },
        df.lsize)(indexColIdOrdering)
    }
  }

  def openTypedDf[T: ClassTag, FileId](dir: Dir[FileId])(implicit tag: TypeTag[T]): TypedDf[T] = {
    TypedDf[T](openDf[String, FileId](dir))
  }

  def createTypedDf[T: ClassTag, FileId](items: Seq[T], dir: WritableDir[FileId])(
    implicit tag: TypeTag[T]): TypedDf[T] = {
    writeTypedDf(items, dir)
    openTypedDf(dir)
  }

  def writeTypedDf[T: ClassTag, FileId](items: Seq[T], dir: WritableDir[FileId])(
    implicit tag: TypeTag[T]) = {
    writeDf(TypedDf[T](items), dir)
  }


    def openCfs[CfsFileId](dataRef:DataRef)(implicit ttag:TypeTag[CfsFileId],
                                            ctag:ClassTag[CfsFileId],
                                            ord:Ordering[CfsFileId]) = {
      val fileSeqType  = types.seqTypeOf[CfsFileId]
      val longSeqType = types.longLSeqType
      val intToId = types.intToId[CfsFileId](ttag) //)(ctag, ord)

      new CfsDir[CfsFileId](dataRef, fileSeqType, longSeqType, intToId)(ctag, ord)
    }

    def openCfs[CfsFileId, DirFileId](dir: Dir[DirFileId], id: Int)(
      implicit ttag:TypeTag[CfsFileId],
      ctag:ClassTag[CfsFileId],
      ord:Ordering[CfsFileId]) : CfsDir[CfsFileId] = {
      using (dir.openRef(dir.id(id))) { ref =>
        openCfs[CfsFileId](ref)
      }
    }

    def openWrittenCfs[CfsFileId : TypeTag : ClassTag](ref:AllocateOnce)(
      implicit ord:Ordering[CfsFileId]) : WrittenCfsDir[CfsFileId] = {
      new WrittenCfsDir[CfsFileId](ref,
                                   types.seqTypeOf[CfsFileId],
                                   types.longLSeqType,
                                   types.intToId(typeTag[CfsFileId]))(
                                   classTag[CfsFileId],
                                   ord)
    }

    def openWrittenCfs[CfsFileId:TypeTag : ClassTag, DirFileId](
       dir: WritableDir[DirFileId], id: Int)(
      implicit ord:Ordering[CfsFileId])
    : WrittenCfsDir[CfsFileId] = scoped { implicit bind =>
      openWrittenCfs[CfsFileId](dir.ref(dir.id(id)))
    }

    def writingCfs[CfsFileId : TypeTag : ClassTag, DirFileId, T](
        dir:WritableDir[DirFileId], id:Int)(
        make:WritableDir[CfsFileId] => T)(
        implicit ord:Ordering[CfsFileId]) = {
      using (openWrittenCfs[CfsFileId, DirFileId](dir, id))(make)
    }

    def usingCfs[CfsFileId : TypeTag : ClassTag, T](
       fileRef:DataRef)(f:Dir[CfsFileId] => T)(
       implicit ord:Ordering[CfsFileId]) : T = {
      using (openCfs(fileRef))(f(_))
    }
    def usingCfs[CfsFileId  : TypeTag : ClassTag, FileId, T](
        dir:Dir[FileId], id:Int)(
        f:Dir[CfsFileId] => T)(
        implicit ord:Ordering[CfsFileId]) : T = {
      using (openCfs(dir, id))(f(_))
    }

    def writeIndexedDf[T : ClassTag, FileId](
      df:IndexedDf[T],
      targetDir:WritableDir[FileId])(
      implicit t : TypeTag[T],
      indexSeqT: TypeTag[LSeq[(String, Any)]]) : Unit = {
      using(openWrittenCfs[Int, FileId](targetDir, 0)) { d =>
        writeDf(df.df, d)
      }
      using(openWrittenCfs[Int, FileId](targetDir, 1)) { d =>
        writeDf(df.indexDf, d)(indexSeqT)
      }
    }

    def writeIndexedDf[T: ClassTag](
                           df:IndexedDf[T],
                           fileRef:AllocateOnce)(
                           implicit t : TypeTag[T],
                           indexSeqT: TypeTag[LSeq[(String, Any)]]) : Unit = {
      using (openWrittenCfs[Int](fileRef)) { dir =>
        writeIndexedDf[T, Int](df, dir)
      }
    }

    def openIndexedDf[T : ClassTag](fileRef:DataRef)(
                         implicit t : TypeTag[T],
                         indexSeqT: TypeTag[LSeq[(String, Any)]]) : IndexedDf[T] = {
      usingCfs(fileRef) { dir : Dir[Int] => openIndexedDf[T, Int](dir) }
    }

    def writeMergedIndexedDf[T : ClassTag, FileId](dfs: Seq[IndexedDf[T]],
                                                   targetDir: WritableDir[FileId])(
                                                   implicit t : TypeTag[T],
                                                   indexedSeqT: TypeTag[LSeq[(String, Any)]]): Unit = {
      writingCfs(targetDir, 0) { d : WritableDir[Int] =>
        writeMergedDf(dfs.map(_.df), d, typeColTypes[T])
      }
      writingCfs(targetDir, 1) { d : WritableDir[Int] =>
        writeMergedDf(dfs.map(_.indexDf), d, indexDfColTypes())(
          indexedSeqT, Ordering.Tuple2(Ordering[String], types.anyOrdering))
      }
    }

    def writeIndexedDf[T: ClassTag, FileId](provideItems: Seq[T],
                                            dir: WritableDir[FileId],
                                            indexConf: IndexConf[String] = IndexConf[String]())(
                                     implicit tag: TypeTag[T]): Unit = {
      //l.info(f"memory before df write: " + Utils.memory)
      writingCfs(dir, 0) { d : WritableDir[Int] =>
        writeTypedDf[T, Int](
          provideItems,
          d)
      }
      //l.info(f"memory before index write: " + Utils.memory)
      usingCfs(dir, 0) { d0 : Dir[Int] =>
        using(openTypedDf[T, Int](d0)) { df =>
          //l.info(f"memory after df open: " + Utils.memory)
          writingCfs(dir, 1) { d : WritableDir[Int] =>
            //l.info(f"memory before index write: " + Utils.memory)
            writeIndex[String, Int](df, d, indexConf)
          }
        }
      }
    }

    def openIndexedDf[T: ClassTag, FileId](dir: Dir[FileId])(
      implicit tag: TypeTag[T]): IndexedDf[T] = {
      usingCfs(dir, 0) { d : Dir[Int] =>
        val df = openTypedDf[T, Int](d)
        usingCfs(dir, 1) { d2 : Dir[Int] =>
          new IndexedDf[T](df,
            openIndex[String, Int] (df, d2))
        }
      }
    }

    def multiTypedDf[T:ClassTag](dfs:Array[TypedDf[T]],
                                 colIdMemRatio:Int = MultiDf.DefaultColIdMemRatio)
                                (implicit tag: TypeTag[T]): TypedDf[T] = {
      TypedDf[T](
        MultiDf.autoClosing(dfs, typeColTypes[T], colIdMemRatio))

    }

    def multiIndexedDf[T:ClassTag](dfs:Array[IndexedDf[T]], colIdMemRatio:Int = MultiDf.DefaultColIdMemRatio)
                                  (implicit tag: TypeTag[T]): IndexedDf[T] = {
      new IndexedDf[T](
        multiTypedDf(dfs.map(_.df), colIdMemRatio),
        MultiDf.autoClosing(dfs.map(_.indexDf), indexDfColTypes(), colIdMemRatio)(indexColIdOrdering))
    }

    def createIndexedDf[T: ClassTag, FileId](
      items: Seq[T],
      dir: WritableDir[FileId],
      indexConf: IndexConf[String] = IndexConf[String]())(
      implicit tag: TypeTag[T]): IndexedDf[T] = {
      writeIndexedDf(items, dir, indexConf)
      openIndexedDf[T, FileId](dir)
    }

    def haveIndexedDf[T: ClassTag, FileId](provideItems: => Seq[T],
                                           dir: WritableDir[FileId],
                                           indexConf: IndexConf[String] = IndexConf[String]())(
                                           implicit tag: TypeTag[T]): IndexedDf[T] = {
      if (!dir.exists(dir.id(0)) || !dir.exists(dir.id(1))) {
        writeIndexedDf[T, FileId](provideItems, dir, indexConf)
      }
      openIndexedDf[T, FileId](dir)
    }

}

/* File system dataframes (you can have dataframes in memory or inside cfs files also) **/
class FsDfs(types:IoTypes)(implicit seqSeqTag : TypeTag[LSeq[IoObject]]) extends Dfs(types) {

  def writingFile[T](file:File)(f : DataOutputStream => T) = {
    using (new DataOutputStream(new FileOutputStream(file)))(f)
  }
  def openingDir[T](dir:File)( f : WritableDir[String] => T) = {
    using (new MMapDir(dir))(f)
  }
  def writingCfsFile[T](file:File)( f : WritableDir[Int] => T) = scoped { implicit bind =>
    openingDir[T](file.getParentFile) { d : WritableDir[String] =>
      val cfs = bind(openWrittenCfs[Int](d.ref(file.getName)))
      f(cfs)
    }
  }
  def openingCfsFile[T](file:File)( f : Dir[Int] => T) = scoped { implicit bind =>
    openingDir[T](file.getParentFile) { d =>
      usingCfs(d.ref(file.getName)) { cfs : Dir[Int] =>
        f(cfs)
      }
    }
  }
  def openingFile[T](file:File)( f : FileRef[String] => T) = scoped { implicit bind =>
    using (new MMapDir(file.getParentFile))( dir => f(dir.ref(file.getName)) )
  }

  def writeFile[T : TypeTag](vs:T, file:File) = {
    writingFile(file) { out =>
      super.write[T](out, vs)
    }
  }
  def openFile[T : TypeTag](file:File)(implicit seqTag: TypeTag[LSeq[T]]) = {
    openingFile(file) { ref =>
      super.open[T](ref)
    }
  }

  def writeTypedDfFile[T: ClassTag](provideItems: Seq[T],
                                    file: File)(
                                   implicit tag: TypeTag[T]): Unit = {
    writingCfsFile(file) { dir =>
      super.writeTypedDf[T, Int](provideItems, dir)
    }
  }

  def openTypedDfFile[T:ClassTag](file: File)(implicit tag: TypeTag[T]) = {
    openingCfsFile(file) { dir =>
      super.openTypedDf[T, Int](dir)
    }
  }
  def createTypedDfFile[T:ClassTag](provideItems: Seq[T],
                                    file: File)(implicit tag: TypeTag[T]) = {
    writeTypedDfFile[T](provideItems, file)
    openTypedDfFile[T](file)
  }

  def writeIndexedDfFile[T: ClassTag](provideItems: Seq[T],
                                      file: File,
                                      indexConf: IndexConf[String] = IndexConf[String]())(
                                    implicit tag: TypeTag[T]): Unit = {
    writingCfsFile(file) { dir =>
      super.writeIndexedDf[T, Int](provideItems, dir, indexConf)
    }
  }
  def openIndexedDfFile[T: ClassTag](file: File)(
    implicit tag: TypeTag[T]) = {
    openingCfsFile(file) { file =>
      super.openIndexedDf[T, Int](file)
    }
  }
  def createIndexedDfFile[T : ClassTag](provideItems: Seq[T],
                                        file: File,
                                        indexConf: IndexConf[String] = IndexConf[String]())(
                                        implicit tag: TypeTag[T]) = {
    writeIndexedDfFile[T](provideItems, file, indexConf)
    openIndexedDfFile[T](file)
  }
  def writeMergedIndexedDfFile[T : ClassTag](srcFiles:Seq[File],
                                             destFile:File)(
                                            implicit t: TypeTag[T]): Unit = {
    using (IoScope.open) { implicit bind =>
      val dfs = srcFiles.map(f => bind(openIndexedDfFile[T](f)))
      writingCfsFile(destFile) { mmapDir =>
        super.writeMergedIndexedDf[T, Int](dfs, mmapDir)
      }
    }
  }
  def openMultiIndexedDfFiles[T : ClassTag](srcFiles:Seq[File], colIdMemRatio:Int=MultiDf.DefaultColIdMemRatio)(implicit tag:TypeTag[T]) = {
    val dfs = srcFiles.map { openIndexedDfFile[T] }.toArray
    multiIndexedDf(  dfs, colIdMemRatio )
  }
/*
  def writeMergedIndexedDfsFile[T : ClassTag](srcFiles:Seq[File], mergeDir:File, targetFile:File)(implicit tag:TypeTag[T]) = {
    var segments = srcFiles

    var id = 0
    var createdSegments = Set[File]()

    while (segments.size > 1) {
      val newSegments = new ArrayBuffer[File]()
      segments.grouped(2).foreach { g =>
        if (g.size == 2) {
          mergeDir.mkdirs()
          val f = new File(mergeDir, f"_$id")
          id += 1
          using(IoScope.open) { implicit bind =>
            l.info("merging " + g(0) + " and " + g(1) + "...")
            val before = System.currentTimeMillis()
            writeMergedIndexedDfFile[T](g(0), g(1), f)
            val ms = System.currentTimeMillis() - before
            l.info("took " + ms + " ms.")
          }
          // cleanup
          val delete = Seq(g(0), g(1))
          delete.filter(createdSegments.contains(_)).foreach { f =>
            l.info("cleaning " + f.getAbsolutePath)
            f.delete()
          }
          createdSegments = (createdSegments -- delete)

          newSegments += f
          createdSegments += f
        } else {
          newSegments += g(0)
        }
      }
      segments = newSegments
    }

    segments.head.renameTo(targetFile)
  }*/

}

object Dfs {

  val l = LoggerFactory.getLogger(this.getClass)

  def makeFs = new FsDfs(IoTypes.strings)
  /* default filesystem dataframes */
  val fs = makeFs

}
*/