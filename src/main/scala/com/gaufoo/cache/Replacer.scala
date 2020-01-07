package com.gaufoo.cache

import com.gaufoo.Config.BlockID

class Replacer {
  private var _list = List[BlockID]()

  def add(blockId: BlockID): Unit =
    _list = _list.appended(blockId)

  def evict: Option[BlockID] =
    if (_list.isEmpty) None
    else {
      val r = _list.head
      _list = _list.tail
      Option(r)
    }

  def remove(blockId: BlockID): Unit = {
    val idx = _list.indexOf(blockId)
    assert(idx >= 0)
    _list = _list.take(idx) ++ _list.drop(idx + 1)
  }
}
