package com.gaufoo

import java.io.File
import java.nio.file.Paths
import java.util.concurrent.Executors

import com.gaufoo.Config.Value
import com.gaufoo.access.Accessor
import com.gaufoo.cache.Cache
import com.gaufoo.pre_process.PreProcessor

import scala.concurrent.ExecutionContext
import scala.util.Random

object Main {
  def preProcess(): Unit = {
    val p = new PreProcessor(new File("/home/gaufoo/Desktop/tmp/test-data.dat"), Paths.get("/home/gaufoo/Desktop/tmp"))
    p.process()
  }

  def main(args: Array[String]): Unit = {
//    preProcess()

    val cache    = new Cache("/home/gaufoo/Desktop/tmp")
    val accessor = new Accessor(cache, 0L)

    /* - multi-thread random read - */
    val ec = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
    for { _ <- 0 to 20 } ec.execute(() => {
      val start = System.currentTimeMillis()
      for { _ <- 0 to 1000000 } {
        val i     = Random.nextInt(10000000)
        val bytes = i.toString.toCharArray.map(_.toByte)
        assert(accessor.get(bytes).get.sameElements(bytes))
      }
      val stop = System.currentTimeMillis()
      println(s"spent: ${stop - start}")
    })

    /* - single-thread random read - */
//    val start = System.currentTimeMillis()
//    for { _ <- 0 until 500000 } {
//      val i     = Random.nextInt(10000000)
//      val bytes = i.toString.toCharArray.map(_.toByte)
//      assert(accessor.get(bytes).get.sameElements(bytes))
//    }
//    val stop = System.currentTimeMillis()
//    println(s"spent: ${stop - start}")

     /* - single-thread seq read - */
//        val start = System.currentTimeMillis()
//        for { i <- 0 until 500000 } {
//          val bytes = i.toString.toCharArray.map(_.toByte)
//          assert(accessor.get(bytes).get.sameElements(bytes))
//        }
//        val stop = System.currentTimeMillis()
//        println(s"spent: ${stop - start}")
  }
}
