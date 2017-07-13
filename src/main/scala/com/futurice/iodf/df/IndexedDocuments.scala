package com.futurice.iodf.df

/**
  * Created by arau on 13.7.2017.
  */
object IndexedDocuments {

  def from(docs:Seq[Document], conf:IndexConf[String]) : IndexedDocuments = {
    Indexed.from(Documents.from(docs), conf)
  }

}
