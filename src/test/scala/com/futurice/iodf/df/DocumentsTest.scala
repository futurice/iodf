package com.futurice.iodf.df

import java.io.File

import com.futurice.iodf.{IoContext, TestUtil, scoped}
import com.futurice.iodf.store.MMapFile
import com.futurice.iodf.df._
import com.futurice.iodf.util.Tracing
import com.futurice.testtoys.{TestSuite, TestTool}

/**
  * Created by arau on 13.7.2017.
  */
class DocumentsTest extends TestSuite("df/documents") {

  import TestUtil._

  val items =
   Seq(Document("name"->"a", "property"->true, "number"->3, "text"->"some text"),
       Document("name"->"b", "property"->false, "bigNumber"->3450324234233242L),
       Document("name"->"c", "number"->3, "text"->"more text here"))

  val indexConf =
    IndexConf[String]()
      .withAnalyzer("text",
        { e => e match {
          case Some(text: String) => text.split(" ") ++ Seq(true)
          case None => Seq(false)
        }} : Any => Seq[Any])


  test("documents") { t =>
    Tracing.trace {
      scoped { implicit bind =>
        val file = MMapFile(new File(t.fileDir, "myDf"))
        implicit val io = IoContext()

        val df = Documents.from(items)
        t.tln
        tDf(t, df)
        tSeq(t, df)
        t.tln
        t.t("writing df..")
        t.iMsLn(file.save(df))
        t.tln
        t.t("opening df..")
        val df2 =
          t.iMsLn(file.as[Documents])
        tDf(t, df2)
        tSeq(t, df2)
      }
    }
    t.tln
    tRefCount(t)
  }

  test("indexed-documents") { t =>
    Tracing.trace {
      scoped { implicit bind =>
        val file = MMapFile(new File(t.fileDir, "myDf"))
        implicit val io = IoContext()

        val df = Indexed.from(Documents.from(items), indexConf)
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
          t.iMsLn(file.as[IndexedDocuments])
        t.tln("documents:")
        t.tln
        tDf(t, df2)
        t.tln("index:")
        t.tln
        tDf(t, df.indexDf)
      }
    }
    t.tln
    tRefCount(t)
  }

}
