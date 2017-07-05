package com.futurice.iodf.io

import com.futurice.iodf.Utils.using

/**
  * Created by arau on 5.7.2017.
  */
class DataRefView(viewed:DataRef,
                  from:Long,
                  until:Long) extends DataRef {
  val v        = viewed.copy
  def close    = v.close
  def copy     = new DataRefView(v, from, until)
  def openAccess     =
    using (v.openAccess) { _.openView(from, until) }
  def byteSize = until - from
  def openView(from:Long, until:Long) = {
    new DataRefView(v, this.from + from, this.from + until)
  }
}
