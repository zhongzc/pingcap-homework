package com.gaufoo.cache

import org.scalatest._

class ReplacerSuite extends FunSuite {
  test("add element") {
    val stack = new Replacer
    stack.add(10L)
    stack.add(20L)
  }

  test("remove element") {
    val stack = new Replacer
    stack.add(10L)
    stack.remove(20L)
    stack.remove(10L)
  }

  test("evict element") {
    val stack = new Replacer
    stack.add(10L)
    stack.add(20L)
    stack.add(30L)
    stack.remove(30L)
    assert(stack.evict === Some(10L))
    assert(stack.evict === Some(20L))
    assert(stack.evict === None)
  }
}
