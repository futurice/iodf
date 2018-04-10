package com.futurice.iodf.io

import com.futurice.iodf._

/**
  * Created by arau on 5.7.2017.
  */
class DataRefView(viewed:DataRef,
                  from:Long,
                  until:Long) extends DataRef {
  def openCopy     = new DataRefView(viewed, from, until)
  def access     = viewed.access.view(from, until)
  def byteSize = until - from
  def view(from:Long, until:Long) = {
    new DataRefView(viewed, this.from + from, this.from + until)
  }
}
