package com.gaufoo.cache

import org.scalatest._

class LRUReplacerSuite extends FunSuite {
  test("add element") {
    val stack = new Replacer
    stack.add(10)
    stack.add(20)
  }

  test("remove element") {
    val stack = new Replacer
    stack.add(10)
    stack.add(20)
    stack.remove(20)
    stack.remove(10)
  }

  test("evict element") {
    val stack = new Replacer
    stack.add(10)
    stack.add(20)
    stack.add(30)
    stack.remove(30)
    assert(stack.evict === Some(10))
    assert(stack.evict === Some(20))
    assert(stack.evict === None)
  }
}
