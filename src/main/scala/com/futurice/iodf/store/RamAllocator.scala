package com.futurice.iodf.store

import com.futurice.iodf.io.DataOutput

/**
  * Created by arau on 5.7.2017.
  */
class RamAllocator extends Allocator {
  override def create: DataOutput = {
    new LBufferCreator
  }
  override def close(): Unit = {}
}
