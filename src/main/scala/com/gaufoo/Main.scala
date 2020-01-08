package com.gaufoo

import java.io.File
import java.nio.file.Paths
import java.util.concurrent.Executors

import com.gaufoo.Config.Value
import com.gaufoo.access.Accessor
import com.gaufoo.cache.Cache
import com.gaufoo.pre_process.PreProcessor

import scala.concurrent.ExecutionContext

object Main {
  def main(args: Array[String]): Unit = {

//    val p = new PreProcessor(new File("/home/gaufoo/Desktop/tmp/bigfile.dat"), Paths.get("/home/gaufoo/Desktop/tmp"))
//    p.process()

    val executionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(10))

    val accessor = new Accessor(new Cache("/home/gaufoo/Desktop/tmp"), 0L)

    for {j <- 0 until 5000000 / 50000} {
      executionContext.execute(() => {
        for (i <- j * 50000 + 0 until j * 50000 + 50000) {
          val a: Option[Value] = accessor.get(i.toString.getBytes())
          if (a.isEmpty) {
            println(i)
          }
        }
        println("done")
      })
    }

  }
}
