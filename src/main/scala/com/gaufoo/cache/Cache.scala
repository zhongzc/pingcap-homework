package com.gaufoo.cache

import java.io.{BufferedInputStream, FileInputStream}
import java.nio.file.Paths
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean

import com.gaufoo.Config._
import com.gaufoo.Utils

import scala.concurrent.ExecutionContext

class Cache(_path: String) {
  private var _freeList  = List.fill(BLOCK_COUNT)(new Block)
  private val _blockPool = new BlockPool
  private val _replacer  = new Replacer

  private val _mutex            = new Object
  private val _executionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))

  def fetch(blockId: BlockID): Option[Block] = _mutex.synchronized {

    // Case 1: the block is cached
    val pooled = _blockPool.get(blockId)
    if (pooled.nonEmpty) {
      val res = pooled.get
      if (res.pinCount == 0)
        _replacer.remove(res.blockId)
      res.pinCount += 1
      return Option(res)
    }

    // Case 2: pick a block from the non-empty free list
    if (_freeList.nonEmpty) {
      val res = _freeList.head
      _freeList = _freeList.tail
      _initBlock(blockId, res)
      return Option(res)
    }

    // Case 3: fetch a block by evicting an inactive block
    val evicted = _replacer.evict
    if (evicted.nonEmpty) {
      val res = _blockPool.remove(evicted.get)
      _initBlock(blockId, res)
      return Option(res)
    }

    None
  }

  def drop(block: Block): Unit = _mutex.synchronized {
    block.pinCount -= 1
    if (block.pinCount == 0)
      _replacer.add(block.blockId)
  }

  private def _initBlock(blockId: BlockID, block: Block): Unit = {
    block.blockId = blockId
    block.pinCount = 1

    val locked = new AtomicBoolean(false)
    _executionContext.execute(() => {
      block.rwLock.writeLock().lock()
      locked.set(true)

      try _buildTreeFromFile(block)
      finally block.rwLock.writeLock().unlock()
    })
    while (!locked.get) { /* Ensure the block is initialized before the following reads. */ }

    _blockPool.add(blockId, block)
  }

  private def _buildTreeFromFile(block: Block): Unit = {
    block.tree.clear()

    val path = Paths.get(_path, block.blockId.toString)
    val bis  = new BufferedInputStream(new FileInputStream(path.toFile))

    def getInt: Int                      = Utils.getInt(bis)
    def getBytes(size: Int): Array[Byte] = Utils.getBytes(bis, size)

    block.blockType = getBytes(1)(0)

    val totalCnt = getInt
    for (_ <- 0 until totalCnt) {
      val keySize   = getInt
      val key       = getBytes(keySize)
      val valueSize = getInt
      val value     = getBytes(valueSize)
      block.tree.put(key, value)
    }

    bis.close()
  }
}
