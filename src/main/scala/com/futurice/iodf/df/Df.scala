package com.futurice.iodf.df

import com.futurice.iodf
import com.futurice.iodf.util.LSeq

/**
  * Data frame definition:
  *
  *   The concept of a data frame comes from the world of statistical software used in empirical research;
  *   it generally refers to "tabular" data: a data structure representing cases (rows), each of which consists
  *   of a number of observations or measurements (columns).
  *
  *   - https://github.com/mobileink/data.frame/wiki/What-is-a-Data-Frame%3F
  *
  * Now IoDf is by its nature:
  *
  *   column-oriented, to maximize speed of column operations and cross-column operations, which
  *   are typically A) most interesting cases for data science and B) easiest to optimize, as long
  *   the columns are strongly typed.
  *
  * We use dataframe in the loose sense of something:
  *
  *   A) that has rows of certain strong type
  *   B) and that has strongly typed columns
  *
  * And that's about it. While the types are strong, we leave the actual definition of the
  * row to be loose defined / generic, in order to support several data frame type of constructs, like:
  *
  *  1) Documents, lacking strong row schema
  *  2) Tables, with strict SQL-like row schema
  *  3) Objects[T] which rows map directly to some class T instances
  *
  * Indexes are not (currently) treated as dataframes, because the index 'rows' are not that
  * interesting, and they are (currently) problematic performance wise. (e.g. if you have million
  * wide index, you will face serious problems, if you try reconstruct index row from index columns)
  *
  * This could be - of course - changed, by storing the rows, when the indexes are made, but these
  * rows may be tricky & expensive to reconstruct on merges.
  *
  * NOTE:
  *
  *   It is a good question, whether the ColId should always be string, or could it be something else
  *   (integer, path).
  */
trait Df[RowType] extends Cols[String] with LSeq[RowType] {
  override def size = lsize.toInt
  override def view(from:Long, until:Long) : Df[RowType] =
    new DfView[RowType](this, from, until)
  override def select(indexes:LSeq[Long]) : Df[RowType] = {
    val self = this
    new Df[RowType] {
      override type ColType[T] = LSeq[T]
      override def colIds = self.colIds
      override def colTypes = self.colTypes
      override def colMeta = self.colMeta
      override def colIdOrdering = self.colIdOrdering
      override def apply(l: Long) = self.apply(indexes(l))
      override def _cols = self._cols.lazyMap { e => e.select(indexes) }
      override def lsize = indexes.lsize
      override def close = self.close
    }
  }
}

class DfView[RowType](df:Df[RowType], from:Long, until:Long)
  extends ColsView[String](df, from, until) with Df[RowType]{
  override def apply(l: Long) = df(l - from)
  override def close = { super.close }
}
