package com.gaufoo.util

import java.io.InputStream
import java.nio.ByteBuffer

object Utils {
  def bytesToInt(bytes: Array[Byte]): Int = {
    val b0: Int = (bytes(0) << 0x18) & 0xFF000000
    val b1: Int = (bytes(1) << 0x10) & 0x00FF0000
    val b2: Int = (bytes(2) << 0x08) & 0x0000FF00
    val b3: Int = (bytes(3) << 0x00) & 0x000000FF
    b0 | b1 | b2 | b3
  }

  def bytesToLong(bytes: Array[Byte]): Long = {
    val b0: Long = (bytes(0) << 0x38) & 0XFF00000000000000L
    val b1: Long = (bytes(1) << 0x30) & 0X00FF000000000000L
    val b2: Long = (bytes(2) << 0x28) & 0X0000FF0000000000L
    val b3: Long = (bytes(3) << 0x20) & 0X000000FF00000000L
    val b4: Long = (bytes(4) << 0x18) & 0X00000000FF000000L
    val b5: Long = (bytes(5) << 0x10) & 0X0000000000FF0000L
    val b6: Long = (bytes(6) << 0x08) & 0X000000000000FF00L
    val b7: Long = (bytes(7) << 0x00) & 0X00000000000000FFL
    b0 | b1 | b2 | b3 | b4 | b5 | b6 | b7
  }

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

  def serializeInt(int: Int): Array[Byte] =
    int.toString.toCharArray.map(_.toByte)
}
