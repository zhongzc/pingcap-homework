package com.gaufoo.pre_process

import java.io._
import java.nio.ByteBuffer
import java.nio.file.{Files, Path, Paths}

import util.control.Breaks._
import com.gaufoo.util.BytesOrdering._
import com.gaufoo.Config._
import com.gaufoo.util.Utils

import scala.collection.mutable

class PreProcessor(file: File, targetPath: Path) {
  def process(): Unit = {
    buildIndex(sort())
    println(s"process(): index construction completed, max KV Count: $maxKVCount")
  }

  private[this] def sort(): Array[File] = {
    val bis = new BufferedInputStream(new FileInputStream(file))

    var readCnt     = 0
    var sortedFiles = List[File]()
    val buffer = mutable.PriorityQueue[(Key, Value)]()(
      implicitly[Ordering[(Key, Value)]].reverse // lowest
    )

    // auxiliary functions: read int & bytes from bis and update states
    def getInt: Int = {
      readCnt += 4
      Utils.getInt(bis)
    }
    def getBytes(size: Int): Array[Byte] = {
      readCnt += size
      Utils.getBytes(bis, size)
    }

    // loop: split the entire file to multiple sorted files
    while (bis.available() > 0) {
      val (key, value) = (getBytes(getInt), getBytes(getInt))
      buffer.enqueue((key, value))

      // while over the thresholds, dump a sorted file
      if (readCnt > SORT_PHASE_THRESHOLDS) {
        sortedFiles = dumpSortedKV(
          buffer,
          Utils.intToBytes(buffer.length),
          sortedFiles.length
        ) :: sortedFiles

        readCnt = 0
        System.gc()
      }
    }
    sortedFiles = dumpSortedKV(
      buffer,
      Utils.intToBytes(buffer.length),
      sortedFiles.length
    ) :: sortedFiles

    bis.close()
    sortedFiles.toArray
  }

  private[this] def dumpSortedKV(
    buffer: mutable.PriorityQueue[(Key, Value)],
    meta: Array[Byte],
    id: Int
  ): File = {
    val filename   = "sorted-" + id.toString
    val sortedFile = targetPath.resolve(filename).toFile
    val bos        = new BufferedOutputStream(new FileOutputStream(sortedFile))

    bos.write(meta)
    while (buffer.nonEmpty) {
      val (key, value) = buffer.dequeue()
      bos.write(Utils.intToBytes(key.length))
      bos.write(key)
      bos.write(Utils.intToBytes(value.length))
      bos.write(value)
    }

    bos.flush()
    bos.close()
    sortedFile
  }

  private[this] def buildIndex(files: Array[File]): Unit = {
    val iss    = files.map(f => new BufferedInputStream(new FileInputStream(f)))
    val counts = iss.map(is => Utils.getInt(is))
    val queue = mutable.PriorityQueue[(Key, Value, Int)]()(
      implicitly[Ordering[(Key, Value, Int)]].reverse // lowest
    )

    // auxiliary functions: retrieve the min kv pair from all sorted files
    def retrieveKV(index: Int): Unit = {
      if (counts(index) == 0) return
      val ks    = Utils.getInt(iss(index))
      val key   = Utils.getBytes(iss(index), ks)
      val vs    = Utils.getInt(iss(index))
      val value = Utils.getBytes(iss(index), vs)
      counts(index) -= 1
      queue.enqueue((key, value, index))
    }
    def getMin: Option[(Key, Value)] =
      if (queue.isEmpty) None
      else {
        val (k, v, i) = queue.dequeue()
        retrieveKV(i)
        Option((k, v))
      }

    // init queue: do a read for each sorted file
    iss.indices.foreach(i => retrieveKV(i))

    // the most vital step:
    //   (1) construct global-ordered KVs and dump into 8k-block files
    //   (2) construct hierarchical index structure
    val internalBlockBuilder         = new InternalBlockBuilder
    var currentBlockID               = 1L
    var nextKV: Option[(Key, Value)] = getMin
    do {
      val (_nextKV, markKey) = dumpLeafBlock(nextKV.get, getMin, currentBlockID)
      nextKV = _nextKV
      currentBlockID = internalBlockBuilder.put(markKey, currentBlockID)
      currentBlockID += 1
    } while (nextKV.isDefined)
    internalBlockBuilder.dumpAllInternalBlock(currentBlockID)

    iss.foreach(_.close())
    files.foreach(_.delete())
  }

  private[this] class InternalBlockBuilder {
    private[this] val leveledBuffers    = mutable.ArrayBuffer[ByteBuffer]()
    private[this] val leveledByteCounts = mutable.ArrayBuffer[Int]()
    private[this] val leveledKVCounts   = mutable.ArrayBuffer[Int]()
    private[this] val leveledMarkKeys   = mutable.ArrayBuffer[Key]()

    def put(markKey: Key, blockID: BlockID): BlockID =
      recHandleHierarchy(0, markKey, blockID)

    def dumpAllInternalBlock(blockID: BlockID): Unit = {
      var bid   = blockID
      var level = 0
      while (level < leveledBuffers.length - 1) {
        dumpInternalBlock(level, bid)
        bid = recHandleHierarchy(level + 1, leveledMarkKeys(level), bid)
        level += 1
      }

      // The ROOT node
      dumpInternalBlock(leveledBuffers.length - 1, 0L)
    }

    private[this] def recHandleHierarchy(
      level: Int,
      markKey: Key,
      indexedBlockId: BlockID
    ): BlockID =
      if (level == leveledBuffers.length) {
        // Case 1:
        //   current level is not initialized
        extendLevel()
        putIndex(level, markKey, indexedBlockId)
        indexedBlockId

      } else if (leveledByteCounts(level) + markKey.length + 16 > BLOCK_SIZE) {
        // Case 2:
        //   current internal index block is full, need
        //   to be dumped and indexed by parent node
        val myBlockID = indexedBlockId + 1
        dumpInternalBlock(level, myBlockID)
        val nextID = recHandleHierarchy(level + 1, leveledMarkKeys(level), myBlockID)

        // reset current node
        resetLevel(level)
        putIndex(level, markKey, indexedBlockId)
        nextID

      } else {
        // Case 3:
        //   nothing special happens
        putIndex(level, markKey, indexedBlockId)
        indexedBlockId
      }

    private[this] def extendLevel(): Unit = {
      leveledBuffers += ByteBuffer.allocate(BLOCK_SIZE - 5)
      leveledByteCounts += 5
      leveledKVCounts += 0
      leveledMarkKeys += null
    }

    private[this] def resetLevel(level: Int): Unit = {
      leveledBuffers(level).clear()
      leveledByteCounts(level) = 5
      leveledKVCounts(level) = 0
      leveledMarkKeys(level) = null
    }

    private[this] def putIndex(level: Int, markKey: Key, indexedBlockID: BlockID): Unit = {
      val buf = leveledBuffers(level)
      buf.put(Utils.intToBytes(markKey.length))
      buf.put(markKey)
      buf.put(Utils.intToBytes(8))
      buf.put(Utils.longToBytes(indexedBlockID))
      leveledByteCounts(level) += markKey.length + 16
      leveledKVCounts(level) += 1
      leveledMarkKeys(level) = markKey
    }

    private[this] def dumpInternalBlock(level: Int, blockID: BlockID): Unit =
      dumpBlock(
        blockID,
        BLK_INTERNAL,
        leveledKVCounts(level),
        leveledBuffers(level)
      )
  }

  private[this] def dumpLeafBlock(
    firstKV: (Key, Value),
    getMin: => Option[(Key, Value)],
    blockID: BlockID
  ): (Option[(Key, Value)], Key) = {
    // meta data include block type (1 Byte) & number of KVs (4 Bytes)
    var byteCount = 5
    val buffer    = ByteBuffer.allocate(BLOCK_SIZE - 5)

    var (k, v)   = firstKV
    var (ks, vs) = (k.length, v.length)
    var markKey  = k
    var KVCount  = 0

    breakable {
      while (byteCount + ks + vs + 8 < BLOCK_SIZE) {
        byteCount += ks + vs + 8
        buffer.putInt(ks)
        buffer.put(k)
        buffer.putInt(vs)
        buffer.put(v)
        markKey = k
        KVCount += 1

        val op = getMin
        if (op.isEmpty) {
          k = null
          v = null
          break
        }

        val (_k, _v) = op.get
        k = _k
        v = _v
        ks = _k.length
        vs = _v.length
      }
    }

    dumpBlock(blockID, BLK_LEAF, KVCount, buffer)
    (for { k <- Option(k); v <- Option(v) } yield (k, v), markKey)
  }

  private[this] def dumpBlock(
    blockID: BlockID,
    blockType: Byte,
    KVCount: Int,
    KVBuffer: ByteBuffer
  ): Unit = {
    updateMaxKVCount(KVCount)

    val blockFile = resolveBlockFile(blockID)
    val os        = new BufferedOutputStream(new FileOutputStream(blockFile))
    os.write(Array(blockType))
    os.write(Utils.intToBytes(KVCount))
    os.write(KVBuffer.array())
    os.flush()
    os.close()
  }

  private[this] def resolveBlockFile(blockID: BlockID): File = {
    val lvl0 = (blockID >> 20) & ((1 << 10) - 1)
    val lvl1 = (blockID >> 10) & ((1 << 10) - 1)
    val lvl2 = blockID & ((1 << 10) - 1)

    val path = targetPath.resolve(lvl0.toString).resolve(lvl1.toString)
    val p    = Files.createDirectories(path)
    p.resolve(lvl2.toString).toFile
  }

  private[this] var maxKVCount = 0
  private[this] def updateMaxKVCount(count: Int): Unit =
    if (count > maxKVCount) maxKVCount = count

}
