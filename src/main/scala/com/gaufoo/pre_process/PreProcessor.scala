package com.gaufoo.pre_process

import java.io._
import java.nio.file.Path

import com.gaufoo.BytesOrdering._
import com.gaufoo.Config._
import com.gaufoo.Utils

import scala.collection.mutable

class PreProcessor(_file: File, _target_path: Path) {
  private val _SORT_PHASE_THRESHOLDS = 1 * G

  def process(): Unit =
    _doBuildIndex(_doSort)

  private def _doSort: List[File] = {
    val bis = new BufferedInputStream(new FileInputStream(_file))

    var readCnt     = 0
    val buffer      = mutable.PriorityQueue[(Key, Value)]()
    var sortedFiles = List[File]()

    def getInt: Int = {
      readCnt += 4
      Utils.getInt(bis)
    }

    def getBytes(size: Int): Array[Byte] = {
      readCnt += size
      Utils.getBytes(bis, size)
    }

    while (bis.available() > 0) {
      val key   = getBytes(getInt)
      val value = getBytes(getInt)
      buffer.enqueue((key, value))

      if (readCnt > _SORT_PHASE_THRESHOLDS) {
        val sortedFile = _target_path.resolve(sortedFiles.length.toString).toFile
        _dump(buffer, Utils.intToBytes(buffer.length), sortedFile)
        sortedFiles = sortedFile :: sortedFiles

        readCnt = 0
        System.gc()
      }
    }

    bis.close()
    sortedFiles
  }

  private def _doBuildIndex(value: List[File]): Unit = {}

  private def _dump(buffer: mutable.PriorityQueue[(Key, Value)], meta: Array[Byte], file: File): Unit = {
    val bos = new BufferedOutputStream(new FileOutputStream(file))

    bos.write(meta)
    while (buffer.nonEmpty) {
      val (key, value) = buffer.dequeue()
      bos.write(Utils.intToBytes(key.length))
      bos.write(key)
      bos.write(Utils.intToBytes(value.length))
      bos.write(value)
    }

    bos.flush()
    bos.close()
  }
}
