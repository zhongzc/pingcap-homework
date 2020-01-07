package com.gaufoo

object Config {
  type Key     = Array[Byte]
  type Value   = Array[Byte]
  type BlockID = Long

  val K: Int = 1024
  val M: Int = 1024 * K
  val G: Int = 1024 * M

  val BLOCK_SIZE: Int  = 4 * K
  val BLOCK_COUNT: Int = (1 * G) / BLOCK_SIZE

  val BLK_UNKNOWN: Byte  = 0
  val BLK_INTERNAL: Byte = 1
  val BLK_LEAF: Byte     = 2
}
