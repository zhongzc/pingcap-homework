package com.gaufoo.access

import com.gaufoo.Config._
import com.gaufoo.cache.{Block, Cache}
import com.gaufoo.util.Utils

import scala.util.Random

class Accessor(cache: Cache, root: BlockID = 0L) {
  def get(key: Key): Option[Value] = {
    var blk = fetchBlock(root)

    blk.rwLock.readLock().lock()
    while (blk.blockType == BLK_INTERNAL) {
      val o = blk.KVs.minAfter(key)
      blk.rwLock.readLock().unlock()
      if (o.isEmpty) return None

      val value = o.get._2
      drop(blk)

      val nextBlockId = Utils.bytesToLong(value)
      blk = fetchBlock(nextBlockId)
      blk.rwLock.readLock().lock()
    }

    assert(blk.blockType == BLK_LEAF)
    val value = blk.KVs.get(key)
    blk.rwLock.readLock().unlock()
    drop(blk)

    value
  }

  private[this] def fetchBlock(blockId: BlockID): Block = {
    var res = cache.fetch(blockId)
    while (res.isEmpty) {
      Thread.sleep(Random.nextLong(1000))
      res = cache.fetch(blockId)
    }
    res.get
  }

  private[this] def drop(block: Block): Unit =
    cache.drop(block)
}
