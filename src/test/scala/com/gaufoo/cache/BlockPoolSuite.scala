package com.gaufoo.cache

import org.scalatest._

class BlockPoolSuite extends FunSuite {
  test("add block") {
    val pool = new BlockPool
    val blk  = new Block
    blk.blockId = 0L
    pool.add(0L, blk)
  }

  test("get block") {
    val pool = new BlockPool
    val blk  = new Block
    blk.blockId = 0L
    pool.add(blk.blockId, blk)
    assert(pool.get(blk.blockId).get === blk)
  }

  test("remove block") {
    val pool = new BlockPool
    val blk  = new Block
    blk.blockId = 0L
    pool.add(blk.blockId, blk)
    assert(pool.get(blk.blockId).get === blk)
    assert(pool.remove(blk.blockId) === blk)
    assert(pool.get(blk.blockId) === None)
  }
}
