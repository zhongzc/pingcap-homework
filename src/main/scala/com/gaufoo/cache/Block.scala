package com.gaufoo.cache

import java.util.concurrent.locks.ReentrantReadWriteLock

import com.gaufoo.util.BytesOrdering.{o => ord}
import com.gaufoo.Config._

import scala.annotation.tailrec

class Block {

  var blockId: BlockID = -1L
  var blockType: Byte  = BLK_UNKNOWN
  var pinCount: Int    = 0
  var KVs              = new SortedArray
  val rwLock           = new ReentrantReadWriteLock

  class SortedArray {
    private[this] var length = 0
    private[this] var keys   = new Array[Key](BLOCK_KV_COUNT_HINT)
    private[this] var values = new Array[Value](BLOCK_KV_COUNT_HINT)

    def minAfter(key: Key): Option[(Key, Value)] =
      searchRec(key).map(i => (keys(i), values(i)))

    def clear(): Unit = {
      for { i <- 0 until length } {
        keys(i) = null
        values(i) = null
      }
      length = 0
    }

    def put(key: Key, value: Value): Unit = {
      if (length == keys.length) growArray()

      keys(length) = key
      values(length) = value
      length += 1
    }

    def get(key: Key): Option[Value] =
      for {
        (k, v) <- minAfter(key)
        if ord.equiv(key, k)
      } yield v

    private[this] def searchRec(key: Key): Option[Int] = {
      @tailrec
      def searchRec(start: Int, end: Int): Int =
        if (start > end) start
        else {
          val middle = (start + end) / 2
          if (ord.equiv(keys(middle), key))
            middle
          else if (ord.lt(keys(middle), key))
            searchRec(middle + 1, end)
          else
            searchRec(start, middle - 1)
        }

      val idx = searchRec(0, length - 1)
      if (idx >= length) None
      else Option(idx)
    }

    private[this] def growArray(): Unit = {
      val newLength = length * 2
      val newKeys   = new Array[Key](newLength)
      val newValues = new Array[Value](newLength)
      Array.copy(keys, 0, newKeys, 0, length)
      Array.copy(values, 0, newValues, 0, length)
      keys = newKeys
      values = newValues
    }

  }
}
