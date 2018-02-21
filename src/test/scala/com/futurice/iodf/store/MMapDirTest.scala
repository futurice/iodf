package com.futurice.iodf.store

import java.nio.file.{Files, NoSuchFileException}

import com.futurice.iodf.IoScope
import org.scalatest.{FlatSpec, Matchers}

class MMapDirTest extends FlatSpec with Matchers {
  implicit val scope = IoScope.open

  "MMapDir" should "throw exception if the root dir does not exist" in {
    val path = Files.createTempFile("not", "a_dir")

    Files.exists(path) shouldBe true
    Files.isDirectory(path) shouldBe false
    try {
      MMapDir(path.toFile).list
      fail("File accepted as directory in MMapDir.list")
    } catch {
      case fne: NoSuchFileException => {
        if (!fne.getMessage.contains("MMapDir does not exist")) {
          throw fne
        }
        // Pass
      }
    }
  }

  


}
