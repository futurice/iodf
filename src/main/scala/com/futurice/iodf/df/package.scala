package com.futurice.iodf
import com.futurice.iodf.io.SizedMerging
import com.futurice.iodf.util.{LSeq, Ref}

/**
  * Created by arau on 12.7.2017.
  */
package object df {

  type IndexedDf[ColId] = Indexed[ColId, Df[ColId]]
  type IndexedDocuments = Indexed[String, Documents]
  type IndexedTable     = Indexed[String, Table]

  // shorter names
  type Objs[T]        = Objects[T]
  type Docs           = Documents

  type IndexedObjs[T] = IndexedObjects[T]
  type IndexedDocs    = IndexedDocuments

}
