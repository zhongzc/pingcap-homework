package com.gaufoo

import com.gaufoo.access.Accessor
import com.gaufoo.cache.Cache

object Main {
  def main(args: Array[String]): Unit = {
    val accessor = new Accessor(new Cache(".."), 0L)
    accessor.get(Array(1, 2, 3))
  }
}
