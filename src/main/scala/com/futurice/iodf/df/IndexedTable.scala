package com.futurice.iodf.df

import scala.reflect.ClassTag


object IndexedTable {

  def from(schema:TableSchema, rows:Seq[Row], conf:IndexConf[String]) : IndexedTable = {
    Indexed.from(Table.from(schema, rows), conf)
  }

}
