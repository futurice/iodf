package com.futurice.iodf

import java.io.{DataOutputStream, File}
import java.util

import com.futurice.testtoys.{TestSuite, TestTool}
import com.futurice.iodf.store.{MMapDir, RamDir, RefCounted}
import com.futurice.iodf.Utils._
import com.futurice.iodf.ioseq._
import com.futurice.iodf.utils.{DenseBits, SparseBits}

import scala.reflect.runtime.universe._
import scala.util.Random
import scala.concurrent.duration._
import scala.concurrent.Await


/**
  * Created by arau on 7.6.2017.
  */
class DfPerf extends TestSuite("perf/df") {

}
