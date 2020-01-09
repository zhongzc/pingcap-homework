package com.gaufoo.cache

import java.nio.file.{Files, Paths}
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

import com.gaufoo.Config._
import com.gaufoo.util.Utils

import scala.concurrent.ExecutionContext

class Cache(path: String) {
  private[this] var freeList  = List.fill(CACHED_BLOCK_COUNT)(new Block)
  private[this] val blockPool = new BlockPool
  private[this] val replacer  = new Replacer

  private[this] val mutex            = new Object
  private[this] val executionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))

  def fetch(blockId: BlockID): Option[Block] = mutex.synchronized {

    // Case 1: the block is cached
    val pooled = blockPool.get(blockId)
    if (pooled.nonEmpty) {
      val res = pooled.get
      if (res.pinCount == 0)
        replacer.remove(res.blockId)
      res.pinCount += 1
      return Option(res)
    }

    // Case 2: pick a block from the non-empty free list
    if (freeList.nonEmpty) {
      val res = freeList.head
      freeList = freeList.tail
      initBlock(blockId, res)
      return Option(res)
    }

    // Case 3: fetch a block by evicting an inactive block
    val evicted = replacer.evict
    if (evicted.nonEmpty) {
      val res = blockPool.remove(evicted.get)
      initBlock(blockId, res)
      return Option(res)
    }

    None
  }

  def drop(block: Block): Unit = mutex.synchronized {
    block.pinCount -= 1
    if (block.pinCount == 0)
      replacer.add(block.blockId)
  }

  private[this] def initBlock(blockId: BlockID, block: Block): Unit = {
    block.blockId = blockId
    block.pinCount = 1

    val locked = new AtomicBoolean(false)
    executionContext.execute(() => {
      block.rwLock.writeLock().lock()
      locked.set(true)

      try loadFile(block)
      finally block.rwLock.writeLock().unlock()
    })
    while (!locked.get) { /* Ensure the block is initialized before the following reads. */ }

    blockPool.add(blockId, block)
  }

  private[this] def loadFile(block: Block): Unit = {
    block.KVs.clear()

    val file       = Paths.get(path, block.blockId.toString)
    val bytes      = Files.readAllBytes(file)
    var byteOffset = 0

    def getInt: Int = {
      byteOffset += 4
      val slice = bytes.slice(byteOffset - 4, byteOffset)
      Utils.bytesToInt(slice)
    }
    def getBytes(size: Int): Array[Byte] = {
      byteOffset += size
      bytes.slice(byteOffset - size, byteOffset)
    }

    block.blockType = bytes(0)
    byteOffset += 1

    val totalCnt = getInt
    for (_ <- 0 until totalCnt) {
      val keySize   = getInt
      val key       = getBytes(keySize)
      val valueSize = getInt
      val value     = getBytes(valueSize)
      block.KVs.put(key, value)
    }
  }
}
