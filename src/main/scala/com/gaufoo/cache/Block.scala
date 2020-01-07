package com.gaufoo.cache

import java.util.concurrent.locks.ReentrantReadWriteLock

import com.gaufoo.BytesOrdering._
import com.gaufoo.Config._

import scala.collection.mutable

class Block {
  var blockId: BlockID                  = -1L
  var blockType: Byte                   = BLK_UNKNOWN
  var pinCount: Int                     = 0
  var tree: mutable.TreeMap[Key, Value] = mutable.TreeMap[Key, Value]()
  val rwLock                            = new ReentrantReadWriteLock
}
