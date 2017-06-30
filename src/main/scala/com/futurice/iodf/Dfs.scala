package com.futurice.iodf

import java.io.{DataOutputStream, File, FileOutputStream}
import java.util

import com.futurice.iodf.Utils._
import com.futurice.iodf.df._
import com.futurice.iodf.io._
import com.futurice.iodf.ioseq.{IoSeq, IoSeqType}
import com.futurice.iodf.util._
import com.futurice.iodf.store._
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.universe._

trait DfColTypes[ColId] {
  def colType(col:ColId) : IoSeqType[_, _ <: LSeq[_], _ <: IoSeq[_]]
  def colType(index:Long) : IoSeqType[_, _ <: LSeq[_], _ <: IoSeq[_]]
}


class Dfs(val types:IoTypes)(implicit val seqSeqTag : TypeTag[Seq[IoObject]]) {

  val l = LoggerFactory.getLogger(this.getClass)

  def typeColTypes[T : ClassTag](implicit tag: TypeTag[T]) = {
    val schema = typeSchema[T]

    new DfColTypes[String] {
      def colType(col: String) : IoSeqType[_, _ <: LSeq[_], _ <: IoSeq[_]] = {
        schema
          .fieldIoTypes.find(_._1 == col)
          .map(
            _._2.asInstanceOf[
              IoSeqType[
                Any,
                LSeq[Any],
                IoSeq[Any]]]).get
      }
      def colType(index: Long) : IoSeqType[_, _ <: LSeq[_], _ <: IoSeq[_]] = {
        schema
          .fieldIoTypes(index.toInt)
          ._2.asInstanceOf[
            IoSeqType[
              Any,
              LSeq[Any],
              IoSeq[Any]]]
    }
    }
  }

  def indexDfColTypes() = {
    new DfColTypes[(String, Any)] {
      def colType(col: (String, Any)) : IoSeqType[_, _ <: LSeq[_], _ <: IoSeq[_]] =
        types.bitsSeqType
      def colType(index:Long) : IoSeqType[_, _ <: LSeq[_], _ <: IoSeq[_]] =
        types.bitsSeqType
    }
  }

  def apply[ColId](_colIds   : LSeq[ColId],
                   _colTypes : LSeq[Type],
                   __cols    : LSeq[LSeq[Any]],
                   _lsize    : Long)(implicit ord: Ordering[ColId]): Df[ColId] = {
    new Df[ColId] {

      override type ColType[T] = LSeq[T]

      override def colIds = _colIds

      override def colTypes = _colTypes

      override def _cols = __cols.map[ColType[_]](_.asInstanceOf[ColType[_]])

      override def lsize = _lsize

      override def close(): Unit = {
        _colIds.close;
        __cols.close
      }
      override def view(from:Long, until:Long) = {
        new DfView[ColId](this, from, until)
      }

      override def colIdOrdering: Ordering[ColId] = ord
    }
  }

    /*  def writeDf[ColId, FileId](t: Seq[(ColId, _ <: IoType[_ <: IoObject])],
                                 cols: Seq[Seq[_ <: Any]],
                                 dir: Dir[FileId])(
                                 implicit colIdSeqTag: TypeTag[Seq[ColId]],
                                 ordering: Ordering[ColId]): Unit = {

        val order = t.map(_._1).zipWithIndex.sortBy(_._1).map(_._2)

        val sz = cols.map(_.size)
        if (sz.min != sz.max) throw new RuntimeException

        val orderedT = order.map(t(_))
        val orderedCols = order.map(cols(_))

        val vd =
          orderedT.zipWithIndex.map(e => (e._2, e._1._2)).zip(orderedCols).map {
            case ((index, vt), data) =>
              val fileRef = FileRef(dir, dir.id(index + 2))
              using(new DataOutputStream(fileRef.openOutput)) { out =>
                vt.writeAny(out, data)
              }
              new IoRefObject[IoSeq[Any]](
                IoRef(vt, fileRef.toDataRef)): IoObject
          }
        try {
          types.writeIoObject(
            orderedT.map(_._1),
            FileRef(dir, dir.id(0)))
          types.writeIoObject(
            vd,
            FileRef(dir, dir.id(1)))
        } finally {
          vd.foreach {
            _.close
          }
        }
      }*/

  def writeDf[ColId, FileId](df:Df[ColId],
                             dir: WritableDir[FileId])(
                             implicit colIdSeqTag: TypeTag[LSeq[ColId]]) : Unit = {
    try {
      l.info("writing column ids...")
      val before2 = System.currentTimeMillis()
      using (dir.openRef(dir.id(0))) { ref =>
        types.written(ref, df.colIds).close
      }
      l.info("column ids written in " + (System.currentTimeMillis() - before2) + " ms")
      l.info("writing columns...")
      val before = System.currentTimeMillis()
      df._cols.iterator.zipWithIndex.map { case (openedCol, index) =>
        using (openedCol) { col => // this opens the column
          if ((index % 10000) == 0 && index > 0)
            l.info("  written " + index + " columns in " + (System.currentTimeMillis() - before) + " ms")

          using(dir.create(dir.id(index+1))) { out =>
            types.write(out, col)
          }
        }
      }.toArray
      l.info("columns written in " + (System.currentTimeMillis() - before) + " ms")
    } finally {
      vd.foreach {
        _.close
      }
    }
  }

  def writeMergedDf[ColId, FileId](
    dfs: Seq[Df[ColId]], dir: Dir[FileId], colTypes:DfColTypes[ColId])(
    implicit colIdSeqTag: TypeTag[Seq[ColId]],
    ordering: Ordering[ColId]): Unit = {
    using (MultiDf.apply[ColId](dfs, colTypes)) { df =>
      writeDf(df, dir, colTypes)
    }
  }


  def write[T:TypeTag](out: DataOutputStream, v:T) = {
    types.write[T](out, v)
  }

  def open[T:TypeTag](ref:DataRef) : T = {
    types.open[T](ref)
  }
  def createDf[ColId, FileId](df : Df[ColId],
                              dir: WritableDir[FileId])(
                       implicit colIdSeqTag: TypeTag[Seq[ColId]]): Df[ColId] = {
    writeDf[ColId, FileId](df, dir)
    openDf(dir)
  }

  def openDf[ColId, FileId](dir: Dir[FileId])(implicit colIdSeqTag: TypeTag[Seq[ColId]],
                                              colIdOrdering: Ordering[ColId])
  : Df[ColId] = scoped { implicit scope =>
    val ioIds =
      types.open[IoSeq[ColId]](
        dir.ref(dir.id(0)))
    val ioCols =
      types.open[IoSeq[IoObject]](
        dir.ref(dir.id(1))).asInstanceOf[IoSeq[IoSeq[Any]]]
    val sz = using(ioCols(0)) {
      _.lsize
    }
    apply(ioIds,
          new LSeq {
            def lsize = ioCols.lsize
            def apply(i:Long) =
              ioCols.
          },
          ioCols,
          sz)
  }

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

  def createIndex[ColId](df: Df[ColId], dir: Dir[FileId], conf: IndexConf[ColId] = IndexConf[ColId]())
                        (implicit colIdSeqTag: TypeTag[Seq[(ColId, Any)]],
                         boolSeqTag: TypeTag[Seq[Boolean]],
                         ord: Ordering[ColId]) = {
    writeIndex(df, dir, conf, dfColTypes(df))
    openIndex(df, dir)
  }

  def writeIndex[ColId](df: Df[ColId], dir: Dir[FileId], conf: IndexConf[ColId] = IndexConf[ColId](), dfColTypes : DfColTypes[ColId])
                       (implicit colIdSeqTag: TypeTag[Seq[(ColId, Any)]],
                        boolSeqTag: TypeTag[Seq[Boolean]],
                        ord: Ordering[ColId]) = {
    val ids = ArrayBuffer[(ColId, Any)]()
    val cols = ArrayBuffer[IoObject]()
    try {
      for (i <- (0 until df.colIds.size)) {
        val id = df.colIds(i)
        if (conf.isAnalyzed(id))
          using(df.openCol[Any](i)) { col =>
            val ordering = dfColTypes.colType(id) match {
              case t: WithValueTypeTag[_] =>
                types.orderingOf(t.valueTypeTag)
            }
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

              val ref =
                types.writeIoObject(
                  LBits(data, df.lsize),
                  dir.ref(dir.id(cols.size + 2)))

              cols +=
                new IoRefObject[IoObject](ref)
            }
          }
      }
      types.writeIoObject(
        ids: Seq[(ColId, Any)],
        FileRef(dir, dir.id(0)))
      types.writeIoObject(
        cols: Seq[IoObject],
        FileRef(dir, dir.id(1)))
    } finally {
      cols.foreach {
        _.close
      }
    }
  }

  def openIndex[ColId, FileId](df: Df[ColId], dir: Dir[FileId])
                               (implicit colIdSeqTag: TypeTag[LSeq[(ColId, Any)]],
                               boolSeqTag: TypeTag[LSeq[Boolean]],
                               ord: Ordering[ColId]) = {
    scoped { implicit bind =>
      val ioIds =
        types.open[LSeq[(ColId, Any)]](
          bind(dir.openRef(dir.id(0))))
      val ioCols =
        types.open[LSeq[LSeq[Any]]](
          bind(dir.openRef(dir.id(1))))
      apply(ioIds, ioCols, df.lsize)(indexColIdOrdering)
    }
  }


  val TypeRef(seqPkg, seqSymbol, anyArgs) = typeOf[scala.Seq[Any]]

  def typeSchema[T: ClassTag](implicit tag: TypeTag[T]) = {
    val t = classTag[T].runtimeClass

    val fields =
      tag.tpe.members.filter(!_.isMethod).map { e =>
        (e.typeSignature,
          e.name.decoded.trim(),
          types.ioTypeOf(
            scala.reflect.runtime.universe.internal.typeRef(
              seqPkg, seqSymbol, List(e.typeSignature))))
      }.toArray.sortBy(_._2)

    new TypeIoSchema[T](
      t,
      types.ioTypeOf(
        scala.reflect.runtime.universe.internal.typeRef(
          seqPkg, seqSymbol, anyArgs)),
      fields)
  }

  def openTypedDf[T: ClassTag, FileId](dir: Dir[FileId])(implicit tag: TypeTag[T]): TypedDf[T] = {
    val t = typeSchema[T]
    new TypedDfView[T](
      openDf[String](dir))
  }

  def createTypedDf[T: ClassTag, FileId](items: Seq[T], dir: Dir[FileId])(
    implicit tag: TypeTag[T]): TypedDf[T] = {
    writeTypedDf(items, dir)
    openTypedDf(dir)
  }

  def writeTypedDf[T: ClassTag, FileId](items: Seq[T], dir: Dir[FileId])(
    implicit tag: TypeTag[T]) = {
    val t = typeSchema[T]
    writeDf[String](
      t.fieldIoTypes,
      t.toColumns(items),
      dir)
  }

  def openCfs(fileRef:FileRef[FileId]) : CfsDir[FileId] = {
    new CfsDir[FileId](fileRef,
      types.idSeqType,
      types.longSeqType,
      types.intToId _)(classTag[FileId], types.idOrdering)
  }
  def openCfs(dir: Dir[FileId], id: Int) : CfsDir[FileId] = {
    openCfs(dir.ref(dir.id(id)))
  }

  def openWrittenCfs(fileRef:FileRef[FileId]) : WrittenCfsDir[FileId] = {
    new WrittenCfsDir[FileId](fileRef,
      types.idSeqType,
      types.longSeqType,
      types.intToId _)(classTag[FileId], types.idOrdering)
  }
  def openWrittenCfs(dir: Dir[FileId], id: Int) : WrittenCfsDir[FileId] = {
    openWrittenCfs(dir.ref(dir.id(id)))
  }

  def writingCfs[T](dir:Dir[FileId], id:Int, make:Dir[FileId] => T) = {
    using (openWrittenCfs(dir, id))(make)
  }

  def usingCfs[T](fileRef:FileRef[FileId])(f:Dir[FileId] => T) : T = {
    using (openCfs(fileRef))(f(_))
  }
  def usingCfs[T](dir:Dir[FileId], id:Int)(f:Dir[FileId] => T) : T = {
    using (openCfs(dir, id))(f(_))
  }

  def writeIndexedDf[T : ClassTag](df:IndexedDf[T],
                        targetDir:Dir[FileId])(
                        implicit t : TypeTag[T],
                                 indexSeqT: TypeTag[Seq[(String, Any)]]) : Unit = {
    using(openWrittenCfs(targetDir, 0)) { d =>
      writeDf(df.df, d, typeColTypes[T])
    }
    using(openWrittenCfs(targetDir, 1)) { d =>
      writeDf(df.indexDf, d, indexDfColTypes())(
        indexSeqT, Ordering.Tuple2(Ordering[String], types.anyOrdering)
      )
    }
  }

  def writeIndexedDf[T: ClassTag](
                         df:IndexedDf[T],
                         fileRef:FileRef[FileId])(
                         implicit t : TypeTag[T],
                         indexSeqT: TypeTag[Seq[(String, Any)]]) : Unit = {
    using (openWrittenCfs(fileRef)) { dir =>
      writeIndexedDf[T](df, dir)
    }
  }

  def openIndexedDf[T : ClassTag](fileRef:FileRef[FileId])(
                       implicit t : TypeTag[T],
                       indexSeqT: TypeTag[Seq[(String, Any)]]) : IndexedDf[T] = {
    usingCfs(fileRef) { openIndexedDf[T](_) }
  }

  def writeMergedIndexedDf[T : ClassTag](dfs: Seq[IndexedDf[T]],
                              targetDir: Dir[FileId])(
                              implicit t : TypeTag[T],
                              indexedSeqT: TypeTag[Seq[(String, Any)]]): Unit = {
    using(openWrittenCfs(targetDir, 0)) { d =>
      writeMergedDf(dfs.map(_.df), d, typeColTypes[T])
    }
    using(openWrittenCfs(targetDir, 1)) { d =>
      writeMergedDf(dfs.map(_.indexDf), d, indexDfColTypes())(
        indexedSeqT, Ordering.Tuple2(Ordering[String], types.anyOrdering))
    }
  }

  def writeIndexedDf[T: ClassTag](provideItems: Seq[T],
                                  dir: Dir[FileId],
                                  indexConf: IndexConf[String] = IndexConf[String]())(
                                   implicit tag: TypeTag[T]): Unit = {
    //l.info(f"memory before df write: " + Utils.memory)
    using(openWrittenCfs(dir, 0)) { d =>
      writeTypedDf[T](
        provideItems,
        d)
    }
    //l.info(f"memory before index write: " + Utils.memory)
    using(openCfs(dir, 0)) { d0 =>
      using(openTypedDf[T](d0)) { df =>
        //l.info(f"memory after df open: " + Utils.memory)
        using(openWrittenCfs(dir, 1)) { d =>
          //l.info(f"memory before index write: " + Utils.memory)
          writeIndex[String](
            df, d, indexConf, dfColTypes(df))
        }
      }
    }
  }

  def openIndexedDf[T: ClassTag](dir: Dir[FileId])(
    implicit tag: TypeTag[T]): IndexedDf[T] = {
    using(openCfs(dir, 0)) { d =>
      val df = openTypedDf[T](d)
      using(openCfs(dir, 1)) { d2 =>
        new IndexedDf[T](df,
          openIndex(df, d2))
      }
    }
  }

  def multiTypedDf[T:ClassTag](dfs:Array[TypedDf[T]],
                               colIdMemRatio:Int = MultiDf.DefaultColIdMemRatio)
                              (implicit tag: TypeTag[T]): TypedDf[T] = {
    new TypedDfView[T](
      MultiDf.autoClosing(dfs, typeColTypes[T], colIdMemRatio))

  }

  def multiIndexedDf[T:ClassTag](dfs:Array[IndexedDf[T]], colIdMemRatio:Int = MultiDf.DefaultColIdMemRatio)
                                (implicit tag: TypeTag[T]): IndexedDf[T] = {
    new IndexedDf[T](
      multiTypedDf(dfs.map(_.df), colIdMemRatio),
      MultiDf.autoClosing(dfs.map(_.indexDf), indexDfColTypes(), colIdMemRatio)(indexColIdOrdering))
  }

  def createIndexedDf[T: ClassTag](items: Seq[T],
                                   dir: Dir[FileId],
                                   indexConf: IndexConf[String] = IndexConf[String]())(
                                    implicit tag: TypeTag[T]): IndexedDf[T] = {
    writeIndexedDf(items, dir, indexConf)
    openIndexedDf[T](dir)
  }

  def haveIndexedDf[T: ClassTag](provideItems: => Seq[T],
                                 dir: Dir[FileId],
                                 indexConf: IndexConf[String] = IndexConf[String]())(
                                  implicit tag: TypeTag[T]): IndexedDf[T] = {
    if (!dir.exists(dir.id(0)) || !dir.exists(dir.id(1))) {
      writeIndexedDf(provideItems, dir, indexConf)
    }
    openIndexedDf[T](dir)
  }


}

/* File system dataframes (you can have dataframes in memory or inside cfs files also) **/
class FsDfs(types:IoTypes)(implicit seqSeqTag : TypeTag[Seq[IoObject]]) extends Dfs(types) {

  def writingFile[T](file:File)(f : DataOutputStream => T) = {
    using (new DataOutputStream(new FileOutputStream(file)))(f)
  }
  def openingDir[T](dir:File)( f : Dir[String] => T) = {
    using (new MMapDir(dir))(f)
  }
  def writingCfsFile[T](file:File)( f : Dir[String] => T) = {
    openingDir[T](file.getParentFile) { d =>
      using (
        new WrittenCfsDir[String, String](d.ref(file.getName),
          types.idSeqType,
          types.longSeqType,
          types.intToId _)(classTag[String], types.idOrdering)
        ) { cfs =>
        f(cfs)
      }
    }
  }
  def openingCfsFile[T](file:File)( f : Dir[String] => T) = {
    openingDir[T](file.getParentFile) { d =>
      using (
        new CfsDir[String, String](
              d.ref(file.getName),
              types.idSeqType,
              types.longSeqType,
              types.intToId _)(classTag[String], types.idOrdering)) { cfs =>
        f(cfs)
      }
    }
  }
  def openingFile[T](file:File)( f : FileDataRef[String] => T) = {
    using (new MMapDir(file.getParentFile))( dir => f(dir.ref(file.getName, 0, None)) )
  }

  def writeSeqFile[T](vs:Seq[T], file:File)(implicit seqTag : TypeTag[Seq[T]]) = {
    writingFile(file) { out =>
      super.writeSeq[T](vs, out)
    }
  }
  def openSeqFile[T](file:File)(implicit seqTag: TypeTag[Seq[T]]) = {
    openingFile(file) { ref =>
      super.openSeq[T](ref)
    }
  }

  def writeTypedDfFile[T: ClassTag](provideItems: Seq[T],
                                    file: File)(
                                   implicit tag: TypeTag[T]): Unit = {
    writingCfsFile(file) { dir =>
      super.writeTypedDf[T](provideItems, dir)
    }
  }

  def openTypedDfFile[T:ClassTag](file: File)(implicit tag: TypeTag[T]) = {
    openingCfsFile(file) { dir =>
      super.openTypedDf[T](dir)
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
      super.writeIndexedDf[T](provideItems, dir, indexConf)
    }
  }
  def openIndexedDfFile[T: ClassTag](file: File)(
    implicit tag: TypeTag[T]) = {
    openingCfsFile(file) { file =>
      super.openIndexedDf[T](file)
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
        super.writeMergedIndexedDf[T](dfs, mmapDir)
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
