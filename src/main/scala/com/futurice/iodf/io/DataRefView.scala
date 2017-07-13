package com.futurice.iodf.io

import com.futurice.iodf._

/**
  * Created by arau on 5.7.2017.
  */
class DataRefView(viewed:DataRef,
                  from:Long,
                  until:Long) extends DataRef {
  val v        = viewed.openCopy
  def close    = v.close
  def openCopy     = new DataRefView(v, from, until)
  def openAccess     =
    using (v.openAccess) { _.openView(from, until) }
  def byteSize = until - from
  def openView(from:Long, until:Long) = {
    new DataRefView(v, this.from + from, this.from + until)
  }
}
