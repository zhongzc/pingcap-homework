package com.gaufoo.access

import com.gaufoo.Config._
import com.gaufoo.Utils
import com.gaufoo.cache.{Block, Cache}

import scala.util.Random

class Accessor(_cache: Cache, _root: BlockID) {
  def get(key: Key): Option[Value] = {
    var blk = _fetchBlock(_root)

    blk.rwLock.readLock().lock()
    while (blk.blockType == BLK_INTERNAL) {
      val (_, value) = blk.tree.minAfter(key).get
      blk.rwLock.readLock().unlock()
      _drop(blk)

      val nextBlockId = Utils.bytesToInt(value)
      blk = _fetchBlock(nextBlockId)
      blk.rwLock.readLock().lock()
    }

    assert(blk.blockType == BLK_LEAF)
    val value = blk.tree.get(key)
    blk.rwLock.readLock().unlock()
    _drop(blk)

    value
  }

  private def _fetchBlock(blockId: BlockID): Block = {
    var res = _cache.fetch(blockId)
    while (res.isEmpty) {
      Thread.sleep(Random.nextLong(1000))
      res = _cache.fetch(blockId)
    }
    res.get
  }

  private def _drop(block: Block): Unit =
    _cache.drop(block)
}
