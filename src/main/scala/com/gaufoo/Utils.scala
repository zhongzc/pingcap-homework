package com.gaufoo

import java.io.InputStream
import java.nio.ByteBuffer

object Utils {
  def bytesToInt(bytes: Array[Byte]): Int =
    ByteBuffer.wrap(bytes).getInt

  def bytesToLong(bytes: Array[Byte]): Long =
    ByteBuffer.wrap(bytes).getLong

  def getInt(is: InputStream): Int = {
    val buffer = new Array[Byte](4)
    assert(is.read(buffer) == 4)
    Utils.bytesToInt(buffer)
  }

  def getBytes(is: InputStream, size: Int): Array[Byte] = {
    val buffer = new Array[Byte](size)
    assert(is.read(buffer) == size)
    buffer
  }

  def intToBytes(int: Int): Array[Byte] = {
    val buffer = ByteBuffer.allocate(4)
    buffer.putInt(int).array()
  }

  def longToBytes(long: Long): Array[Byte] = {
    val buffer = ByteBuffer.allocate(8)
    buffer.putLong(long).array()
  }
}
