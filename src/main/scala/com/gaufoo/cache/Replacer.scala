package com.gaufoo.cache

import com.gaufoo.Config._

import scala.collection.mutable

class Replacer {
  private[this] case class DLLEntry(blockID: BlockID, var left: DLLEntry, var right: DLLEntry)
  private[this] val start = DLLEntry(-1L, null, null)
  private[this] val end   = DLLEntry(-1L, start, null)
  start.right = end
  private[this] val table = mutable.HashMap[BlockID, DLLEntry]()
  table.sizeHint(CACHED_BLOCK_COUNT)

  def add(blockId: BlockID): Unit = {
    val newNode = DLLEntry(blockId, start, start.right)
    start.right.left = newNode
    start.right = newNode
    table.put(blockId, newNode)
  }

  def evict: Option[BlockID] =
    if (start.right eq end) None
    else {
      val evicted = end.left
      evicted.left.right = end
      end.left = evicted.left
      table.remove(evicted.blockID)
      Option(evicted.blockID)
    }

  def remove(blockId: BlockID): Unit = {
    val r = table.get(blockId)
    if (r.isDefined) {
      val removed = r.get
      removed.left.right = removed.right
      removed.right.left = removed.left
    }
  }
}
