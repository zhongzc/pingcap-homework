package com.gaufoo

object Config {
  type Key     = Array[Byte]
  type Value   = Array[Byte]
  type BlockID = Long

  val K: Int = (1 << 10)
  val M: Int = (1 << 10) * K
  val G: Int = (1 << 10) * M

  val BLOCK_SIZE: Int          = 8 * K
  val CACHED_BLOCK_COUNT: Int  = (20 * M) / BLOCK_SIZE
  val BLOCK_KV_COUNT_HINT: Int = 512

  val BLK_UNKNOWN: Byte  = 0
  val BLK_INTERNAL: Byte = 1
  val BLK_LEAF: Byte     = 2

  val SORT_PHASE_THRESHOLDS: Int = 1 * G
}
