package com.gaufoo.cache

import com.gaufoo.Config._

import scala.collection.mutable

class BlockPool {
  private val _pool = mutable.Map[BlockID, Block]()

  def add(blockId: BlockID, block: Block): Unit =
    _pool.put(blockId, block)

  def get(blockId: BlockID): Option[Block] =
    _pool.get(blockId)

  def remove(blockId: BlockID): Block =
    _pool.remove(blockId).get
}
