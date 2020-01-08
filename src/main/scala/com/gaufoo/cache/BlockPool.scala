package com.gaufoo.cache

import com.gaufoo.Config._

import scala.collection.mutable

class BlockPool {
  private val pool = mutable.Map[BlockID, Block]()

  def add(blockId: BlockID, block: Block): Unit =
    pool.put(blockId, block)

  def get(blockId: BlockID): Option[Block] =
    pool.get(blockId)

  def remove(blockId: BlockID): Block =
    pool.remove(blockId).get
}
