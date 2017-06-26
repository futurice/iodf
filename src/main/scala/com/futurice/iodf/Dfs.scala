package com.futurice.iodf

import java.io.{DataOutputStream, File, FileOutputStream}
import java.util

import com.futurice.iodf.Utils.using
import com.futurice.iodf.utils._
import com.futurice.iodf.store._
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.{ClassTag, classTag}
import scala.reflect.runtime.universe._

trait DfColTypes[IoId, ColId] {
  def colType(col:ColId) : IoSeqType[IoId, _, _ <: LSeq[_], _ <: IoSeq[IoId, _]]
  def colType(index:Long) : IoSeqType[IoId, _, _ <: LSeq[_], _ <: IoSeq[IoId, _]]
}


class Dfs[IoId : ClassTag](val types:IoTypes[IoId])(implicit val seqSeqTag : TypeTag[Seq[IoObject[IoId]]]) {

  val l = LoggerFactory.getLogger(this.getClass)

  def dfColTypes[ColId](df:Df[IoId, ColId]) = {
    new DfColTypes[IoId, ColId] {
      def colType(col: ColId) : IoSeqType[IoId, _, _ <: LSeq[_], _ <: IoSeq[IoId, _]] = {
        df.colType(col)
      }
      def colType(index: Long) : IoSeqType[IoId, _, _ <: LSeq[_], _ <: IoSeq[IoId, _]] = {
        df.colType(index)
      }
    }
  }

  def typeColTypes[T : ClassTag](implicit tag: TypeTag[T]) = {
    val schema = typeSchema[T]

    new DfColTypes[IoId, String] {
      def colType(col: String) : IoSeqType[IoId, _, _ <: LSeq[_], _ <: IoSeq[IoId, _]] = {
        schema
          .fieldIoTypes.find(_._1 == col)
          .map(
            _._2.asInstanceOf[
              IoSeqType[IoId,
                Any,
                LSeq[Any],
                IoSeq[IoId, Any]]]).get
      }
      def colType(index: Long) : IoSeqType[IoId, _, _ <: LSeq[_], _ <: IoSeq[IoId, _]] = {
        schema
          .fieldIoTypes(index.toInt)
          ._2.asInstanceOf[
            IoSeqType[IoId,
              Any,
              LSeq[Any],
              IoSeq[IoId, Any]]]
    }
    }
  }

  def indexDfColTypes() = {
    new DfColTypes[IoId, (String, Any)] {
      def colType(col: (String, Any)) : IoSeqType[IoId, _, _ <: LSeq[_], _ <: IoSeq[IoId, _]] =
        types.bitsSeqType
      def colType(index:Long) : IoSeqType[IoId, _, _ <: LSeq[_], _ <: IoSeq[IoId, _]] =
        types.bitsSeqType
    }
  }

  def apply[ColId](_colIds: IoSeq[IoId, ColId],
                   __cols: IoSeq[IoId, IoSeq[IoId, Any]],
                   _lsize: Long)(implicit ord: Ordering[ColId]): Df[IoId, ColId] = {
    new Df[IoId, ColId] {

      override type ColType[T] = IoSeq[IoId, T]

      override def colIds = _colIds

      override def _cols = __cols

      override def lsize = _lsize

      override def close(): Unit = {
        _colIds.close;
        __cols.close
      }
      override def view(from:Long, until:Long) = {
        new DfView(this, from, until)
      }

      override def colIdOrdering: Ordering[ColId] = ord
    }
  }

/*  def writeMergedDf[ColId](a: Df[IoId, ColId], b: Df[IoId, ColId], dir: Dir[IoId], merging:DfColTypes[IoId, ColId])(
    implicit colIdSeqTag: TypeTag[Seq[ColId]],
    ordering: Ordering[ColId]): Unit = {
    val newColIds = new ArrayBuffer[ColId]()
    val aIndex = new ArrayBuffer[Long]()
    val bIndex = new ArrayBuffer[Long]()
    val aIds = a.colIds
    val bIds = b.colIds
    var aAt = 0L
    var bAt = 0L
    while (aAt < aIds.size && bAt < bIds.size) {
      val aId = aIds(aAt)
      val bId = bIds(bAt)
      val o = ordering.compare(aId, bId)
      if (o == 0) {
        aIndex += aAt
        bIndex += bAt
        newColIds += aId
        aAt += 1
        bAt += 1
      } else if (o < 0) {
        aIndex += aAt
        bIndex += -1
        newColIds += aId
        aAt += 1
      } else {
        aIndex += -1
        bIndex += bAt
        newColIds += bId
        bAt += 1
      }
    }
    while (aAt < aIds.size) {
      aIndex += aAt
      bIndex += -1
      newColIds += aIds(aAt)
      aAt += 1
    }
    while (bAt < bIds.size) {
      aIndex += -1
      bIndex += bAt
      newColIds += bIds(bAt)
      bAt += 1
    }

    val vd =
      newColIds.zipWithIndex.map { case (colId, index) =>
        val fileRef = FileRef(dir, dir.id(index + 2))
        val t = merging.colType(colId)
        using(new DataOutputStream(fileRef.openOutput)) { out =>
          (aIndex(index), bIndex(index)) match {
            case (-1, bi) =>
              using(b.openCol(bi)) { bSeq =>
                t.writeAnyMerged(out, t.defaultSeq(a.lsize), bSeq)
              }
            case (ai, -1) =>
              using(a.openCol(ai)) { aSeq =>
                t.writeAnyMerged(out, aSeq, t.defaultSeq(b.lsize))
              }
            case (ai, bi) =>
              using(a.openCol(ai)) { aSeq =>
                using(b.openCol(bi)) { bSeq =>
                  t.writeAnyMerged(out, aSeq, bSeq)
                }
              }
          }
        }
        new IoRefObject[IoId, IoSeq[IoId, _]](
          IoRef(t, fileRef.toDataRef)): IoObject[IoId]
      }
    try {
      types.writeIoObject(
        newColIds: Seq[ColId],
        FileRef(dir, dir.id(0)))
      types.writeIoObject(
          vd: Seq[IoObject[IoId]],
        FileRef(dir, dir.id(1)))
    } finally {
      vd.foreach {
        _.close
      }
    }
  }*/

  def writeDf[ColId](df:Df[IoId, ColId], dir: Dir[IoId], colTypes:DfColTypes[IoId, ColId])(
    implicit colIdSeqTag: TypeTag[Seq[ColId]],
    ordering: Ordering[ColId]) : Unit = {
    l.info("writing columns...")
    val before = System.currentTimeMillis()

    val vd =
      df._cols.iterator.zipWithIndex.map { case (openedCol, index) =>
        using (openedCol) { col => // this opens the column
          if ((index % 10000) == 0 && index > 0) l.info("  written " + index + " columns in " + (System.currentTimeMillis() - before) + " ms")
          val fileRef = FileRef(dir, dir.id(index + 2))
          val t = colTypes.colType(index)
          using(new DataOutputStream(fileRef.openOutput)) { out =>
            t.writeAnySeq(out, col)
          }
          new IoRefObject[IoId, IoSeq[IoId, _]](
            IoRef(t, fileRef.toDataRef)): IoObject[IoId]
        }
      }.toArray
    l.info("columns written in " + (System.currentTimeMillis() - before) + " ms")
    try {
      l.info("writing column ids...")
      val before2 = System.currentTimeMillis()
      using(FileRef(dir, dir.id(0)).openOutput) { out =>
        val colIdSeqType = types.seqTypeOf[ColId]
        colIdSeqType.writeSeq(new DataOutputStream(out), df.colIds)
      }
      l.info("column ids written in " + (System.currentTimeMillis() - before2) + " ms")
      l.info("writing column references...")
      val before3 = System.currentTimeMillis()
      using(FileRef(dir, dir.id(1)).openOutput) { out =>
        types.refSeqType.writeSeq(new DataOutputStream(out), LSeq(vd.toSeq))
      }
      l.info("column references written in " + (System.currentTimeMillis() - before3) + " ms")
    } finally {
      vd.foreach {
        _.close
      }
    }
  }

  def writeMergedDf[ColId](dfs: Seq[Df[IoId, ColId]], dir: Dir[IoId], colTypes:DfColTypes[IoId, ColId])(
    implicit colIdSeqTag: TypeTag[Seq[ColId]],
    ordering: Ordering[ColId]): Unit = {
    using (MultiDf.apply[IoId, ColId](dfs, colTypes)) { df =>
      writeDf(df, dir, colTypes)
    }
  }


  def writeSeq[T](vs: Seq[T], out: DataOutputStream)(implicit seqTag: TypeTag[Seq[T]]) = {
    val t = types.ioTypeOf[Seq[T]]
    t.write(out, vs)
    t
  }

  def openSeq[T](ref: DataRef[IoId])(implicit seqTag: TypeTag[Seq[T]]) = {
    val t = types.ioTypeOf[Seq[T]]
    using(ref.open) { d =>
      t.open(d).asInstanceOf[IoSeq[IoId, T]]
    }
  }

  def writeDf[ColId](t: Seq[(ColId, _ <: IoType[IoId, _ <: IoObject[IoId]])],
                     cols: Seq[Seq[_ <: Any]],
                     dir: Dir[IoId])(
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
          new IoRefObject[IoId, IoSeq[IoId, Any]](
            IoRef(vt, fileRef.toDataRef)): IoObject[IoId]
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
  }

  def createDf[ColId](t: Seq[(ColId, _ <: IoType[IoId, _ <: IoObject[IoId]])],
                      cols: Seq[Seq[_ <: Any]],
                      dir: Dir[IoId])(
                       implicit colIdSeqTag: TypeTag[Seq[ColId]],
                       ordering: Ordering[ColId]): Df[IoId, ColId] = {
    writeDf(t, cols, dir)
    openDf(dir)
  }

  def openDf[ColId](dir: Dir[IoId])(implicit colIdSeqTag: TypeTag[Seq[ColId]],
                                    colIdOrdering: Ordering[ColId])
  : Df[IoId, ColId] = {
    val ioIds =
      types.openIoObject[Seq[ColId]](
        FileRef(dir, dir.id(0))).asInstanceOf[IoSeq[IoId, ColId]]
    val ioCols =
      types.openIoObject[Seq[IoObject[IoId]]](
        FileRef(dir, dir.id(1))).asInstanceOf[IoSeq[IoId, IoSeq[IoId, Any]]]
    val sz = using(ioCols(0)) {
      _.lsize
    }
    apply(ioIds, ioCols, sz)
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

  def createIndex[ColId](df: Df[IoId, ColId], dir: Dir[IoId], conf: IndexConf[ColId] = IndexConf[ColId]())
                        (implicit colIdSeqTag: TypeTag[Seq[(ColId, Any)]],
                         boolSeqTag: TypeTag[Seq[Boolean]],
                         ord: Ordering[ColId]) = {
    writeIndex(df, dir, conf, dfColTypes(df))
    openIndex(df, dir)
  }

  def writeIndex[ColId](df: Df[IoId, ColId], dir: Dir[IoId], conf: IndexConf[ColId] = IndexConf[ColId](), dfColTypes : DfColTypes[IoId, ColId])
                       (implicit colIdSeqTag: TypeTag[Seq[(ColId, Any)]],
                        boolSeqTag: TypeTag[Seq[Boolean]],
                        ord: Ordering[ColId]) = {
    val ids = ArrayBuffer[(ColId, Any)]()
    val cols = ArrayBuffer[IoObject[IoId]]()
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
                new IoRefObject[IoId, IoObject[IoId]](ref)
            }
          }
      }
      types.writeIoObject(
        ids: Seq[(ColId, Any)],
        FileRef(dir, dir.id(0)))
      types.writeIoObject(
        cols: Seq[IoObject[IoId]],
        FileRef(dir, dir.id(1)))
    } finally {
      cols.foreach {
        _.close
      }
    }
  }

  def openIndex[ColId](df: Df[IoId, ColId], dir: Dir[IoId])
                      (implicit colIdSeqTag: TypeTag[Seq[(ColId, Any)]],
                       boolSeqTag: TypeTag[Seq[Boolean]],
                       ord: Ordering[ColId]) = {
    val ioIds =
      types.openIoObject[Seq[(ColId, Any)]](
        FileRef(dir, dir.id(0))).asInstanceOf[IoSeq[IoId, (ColId, Any)]]
    val ioCols =
      types.openIoObject[Seq[IoObject[IoId]]](
        FileRef(dir, dir.id(1))).asInstanceOf[IoSeq[IoId, IoSeq[IoId, Any]]]
    apply(ioIds, ioCols, df.lsize)(indexColIdOrdering)
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

    new TypeIoSchema[IoId, T](
      t,
      types.ioTypeOf(
        scala.reflect.runtime.universe.internal.typeRef(
          seqPkg, seqSymbol, anyArgs)),
      fields)
  }

  def openTypedDf[T: ClassTag](dir: Dir[IoId])(implicit tag: TypeTag[T]): TypedDf[IoId, T] = {
    val t = typeSchema[T]
    new TypedDfView[IoId, T](
      openDf[String](dir))
  }

  def createTypedDf[T: ClassTag](items: Seq[T], dir: Dir[IoId])(
    implicit tag: TypeTag[T]): TypedDf[IoId, T] = {
    writeTypedDf(items, dir)
    openTypedDf(dir)
  }

  def writeTypedDf[T: ClassTag](items: Seq[T], dir: Dir[IoId])(
    implicit tag: TypeTag[T]) = {
    val t = typeSchema[T]
    writeDf[String](
      t.fieldIoTypes,
      t.toColumns(items),
      dir)
  }

  def openCfs(dir: Dir[IoId], id: Int) = {
    new CfsDir[IoId, IoId](dir.ref(dir.id(id)),
      types.idSeqType,
      types.longSeqType,
      types.intToId _)(classTag[IoId], types.idOrdering)
  }

  def openWrittenCfs(dir: Dir[IoId], id: Int) = {
    new WrittenCfsDir[IoId, IoId](dir.ref(dir.id(id)),
      types.idSeqType,
      types.longSeqType,
      types.intToId _)(classTag[IoId], types.idOrdering)
  }

  def writingCfs[T](dir:Dir[IoId], id:Int, make:Dir[IoId] => T) = {
    using (openWrittenCfs(dir, id))(make)
  }

  def usingCfs[T](dir:Dir[IoId], id:Int, open:Dir[IoId] => T) = {
    using (openCfs(dir, id))(open)
  }

  def writeMergedIndexedDf[T](dfs: Seq[IndexedDf[IoId, T]],
                              targetDir: Dir[IoId])(
                               implicit t: TypeTag[Seq[(String, Any)]]): Unit = {
    using(openWrittenCfs(targetDir, 0)) { d =>
      writeMergedDf(dfs.map(_.df), d, dfColTypes(dfs.head.df))
    }
    using(openWrittenCfs(targetDir, 1)) { d =>
      writeMergedDf(dfs.map(_.indexDf), d, indexDfColTypes())(
        t, Ordering.Tuple2(Ordering[String], types.anyOrdering))
    }
  }

  def writeIndexedDf[T: ClassTag](provideItems: Seq[T],
                                  dir: Dir[IoId],
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

  def openIndexedDf[T: ClassTag](dir: Dir[IoId])(
    implicit tag: TypeTag[T]): IndexedDf[IoId, T] = {
    using(openCfs(dir, 0)) { d =>
      val df = openTypedDf[T](d)
      using(openCfs(dir, 1)) { d2 =>
        new IndexedDf[IoId, T](df,
          openIndex(df, d2))
      }
    }
  }

  def multiTypedDf[T:ClassTag](dfs:Array[TypedDf[IoId,T]],
                               colIdMemRatio:Int = MultiDf.DefaultColIdMemRatio)
                              (implicit tag: TypeTag[T]): TypedDf[IoId, T] = {
    new TypedDfView[IoId, T](
      MultiDf.autoClosing(dfs, typeColTypes[T], colIdMemRatio))

  }

  def multiIndexedDf[T:ClassTag](dfs:Array[IndexedDf[IoId, T]], colIdMemRatio:Int = MultiDf.DefaultColIdMemRatio)
                                (implicit tag: TypeTag[T]): IndexedDf[IoId, T] = {
    new IndexedDf[IoId, T](
      multiTypedDf(dfs.map(_.df), colIdMemRatio),
      MultiDf.autoClosing(dfs.map(_.indexDf), indexDfColTypes(), colIdMemRatio)(indexColIdOrdering))
  }

  def createIndexedDf[T: ClassTag](items: Seq[T],
                                   dir: Dir[IoId],
                                   indexConf: IndexConf[String] = IndexConf[String]())(
                                    implicit tag: TypeTag[T]): IndexedDf[IoId, T] = {
    writeIndexedDf(items, dir, indexConf)
    openIndexedDf[T](dir)
  }

  def haveIndexedDf[T: ClassTag](provideItems: => Seq[T],
                                 dir: Dir[IoId],
                                 indexConf: IndexConf[String] = IndexConf[String]())(
                                  implicit tag: TypeTag[T]): IndexedDf[IoId, T] = {
    if (!dir.exists(dir.id(0)) || !dir.exists(dir.id(1))) {
      writeIndexedDf(provideItems, dir, indexConf)
    }
    openIndexedDf[T](dir)
  }


}

/* File system dataframes (you can have dataframes in memory or inside cfs files also) **/
class FsDfs(types:IoTypes[String])(implicit seqSeqTag : TypeTag[Seq[IoObject[String]]]) extends Dfs[String](types) {

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
  def openingFile[T](file:File)( f : DataRef[String] => T) = {
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
