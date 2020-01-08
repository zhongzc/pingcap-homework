package com.gaufoo.cache

import com.gaufoo.Config.BlockID

import scala.collection.mutable

class Replacer {
  private[this] val list = mutable.ArrayDeque[BlockID]()

  def add(blockId: BlockID): Unit =
    list += blockId

  def evict: Option[BlockID] =
    if (list.isEmpty) None
    else {
      val r = list.removeHead()
      Option(r)
    }

  def remove(blockId: BlockID): Unit = {
    list.removeFirst(blockId == _)
  }
}
