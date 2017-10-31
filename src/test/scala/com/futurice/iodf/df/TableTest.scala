package com.futurice.iodf.df

import java.io.File

import com.futurice.iodf.TestUtil._
import com.futurice.iodf.store.MMapFile
import com.futurice.iodf.{IoContext, scoped}
import com.futurice.iodf.util.{Key, LSeq, Tracing}
import com.futurice.testtoys.TestSuite

/**
  * Created by arau on 13.7.2017.
  */
class TableTest extends TestSuite("df/table") {

  val comment =
    Key[String]("comment")

  val schema =
    TableSchema()
      .withCol[String]("id")
      .withCol[String]("text")
      .withCol[Boolean]("property")
      .withCol[Option[Int]]("optionInt", comment -> "cake")

  val items = Array(
    Row("a", "text here",        true,  Some(4)),
    Row("b", "some text",        false, None),
    Row("c", "some things here", true,  Some(5))
  )

  val indexConf =
    IndexConf[String]()
      .withAnalyzer("text",
        { e => e match {
          case text: String => text.split(" ")
        }} : Any => Seq[Any])

  test("table") { t =>
    Tracing.trace {
      scoped { implicit bind =>
        val file = MMapFile(new File(t.fileDir, "myDf"))
        implicit val io = IoContext()

        val df = bind(Table.from(schema, items))

        t.tln
        tDf(t, df)
        tSeq(t, df)
        t.tln
        t.t("writing df..")
        t.iMsLn(file.save(df))
        t.tln
        t.t("opening df..")
        val df2 =
          t.iMsLn(file.as[Table])
        tDf(t, df2)
        tSeq(t, df2)
      }
    }
  }

  test("indexed-table") { t =>
    Tracing.trace {
      scoped { implicit bind =>
        val file = MMapFile(new File(t.fileDir, "myDf"))
        implicit val io = IoContext()

        val df = bind(IndexedTable.from(schema, items, indexConf))
        t.tln
        t.tln("documents:")
        t.tln
        tDf(t, df)
        t.tln("index:")
        t.tln
        tDf(t, df.indexDf)
        t.t("writing df..")
        t.iMsLn(file.save(df))
        t.tln
        t.t("opening df..")
        val df2 =
          t.iMsLn(file.as[IndexedTable])
        t.tln("documents:")
        t.tln
        tDf(t, df2)
        t.tln("index:")
        t.tln
        tDf(t, df.indexDf)
      }
    }
    t.tln
  }

  test("select") { t =>
    Tracing.trace {
      scoped { implicit bind =>
        val file = MMapFile(new File(t.fileDir, "myDf"))
        implicit val io = IoContext()

        val df = bind(IndexedTable.from(schema, items, indexConf))
        t.tln
        t.tln("df.select result")
        tDf(t, bind(df.openSelect(LSeq(2L, 0L))))
        t.tln
        t.tln("df.indexDf.select result")
        tDf(t, bind(df.indexDf.openSelect(LSeq(2L, 0L))))
        t.tln
        t.tln("df.indexDf.selectSome result")
        tDf(t, bind(df.indexDf.openSelectSome(LSeq(Some(1L), None, Some(2L), Some(1L)))))
        t.tln
      }
    }
    t.tln
    tRefCount(t)
  }


}