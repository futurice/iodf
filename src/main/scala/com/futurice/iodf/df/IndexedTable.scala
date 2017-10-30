package com.futurice.iodf.df

import scala.reflect.ClassTag


object IndexedTable {

  def from(schema:TableSchema, rows:Seq[Row], conf:IndexConf[String]) : IndexedTable = {
    IndexedDf.from(Table.from(schema, rows), conf)
  }

}
