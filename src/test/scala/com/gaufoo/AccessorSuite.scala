package com.gaufoo

import java.io.FileOutputStream
import java.nio.file.Files
import java.util.concurrent.Executors

import com.gaufoo.access.Accessor
import com.gaufoo.cache.Cache
import com.gaufoo.pre_process.PreProcessor
import com.gaufoo.util.Utils
import org.scalatest._

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.reflect.io.Directory
import scala.util.Random

class AccessorSuite extends FunSuite with BeforeAndAfterAll with BeforeAndAfter {
  private val testPath           = Files.createTempDirectory("hw-")
  private val testFile           = Files.createTempFile("hw-", ".dat")
  private var accessor: Accessor = _

  override def beforeAll(): Unit = {
    val os = new FileOutputStream(testFile.toFile)
    for { i <- 1000 until 200000 } {
      val kv  = Utils.serializeInt(i)
      val len = kv.length
      os.write(Utils.intToBytes(len))
      os.write(kv)
      os.write(Utils.intToBytes(len))
      os.write(kv)
    }
    os.close()

    val pp = new PreProcessor(testFile.toFile, testPath)
    pp.process()
  }

  override def afterAll(): Unit = {
    Files.delete(testFile)
    new Directory(testPath.toFile).deleteRecursively()
  }

  before {
    val cache = new Cache(testPath.toAbsolutePath.toString)
    accessor = new Accessor(cache)
  }

  test("single-threaded sequential read") {
    for { i <- 1000 until 200000 } {
      val bytes = Utils.serializeInt(i)
      val res   = accessor.get(bytes)
      assert(res.isDefined)
      assert(res.get.sameElements(bytes))
    }
  }

  test("single-threaded random read") {
    for { _ <- 0 until 200000 } {
      val i     = Random.nextInt(200000 - 1000) + 1000
      val bytes = Utils.serializeInt(i)
      val res   = accessor.get(bytes)
      assert(res.isDefined)
      assert(res.get.sameElements(bytes))
    }
  }

  test("multi-threaded sequential read") {
    val ec = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
    val fs = (0 until 10).map(
      _ =>
        Future {
          for { i <- 1000 until 200000 } {
            val bytes = Utils.serializeInt(i)
            val res   = accessor.get(bytes)
            assert(res.isDefined)
            assert(res.get.sameElements(bytes))
          }
        }(ec)
    )

    for (f <- fs) {
      val res = Await.ready(f, Duration.Inf)
      assert(res.isCompleted)
      assert(res.value.get.isSuccess)
    }
  }

  test("multi-threaded random read") {
    val ec = ExecutionContext.fromExecutor(Executors.newCachedThreadPool())
    val fs = (0 until 10).map(
      _ =>
        Future {
          for { _ <- 0 until 200000 } {
            val i     = Random.nextInt(200000 - 1000) + 1000
            val bytes = Utils.serializeInt(i)
            val res   = accessor.get(bytes)
            assert(res.isDefined)
            assert(res.get.sameElements(bytes))
          }
        }(ec)
    )

    for (f <- fs) {
      val res = Await.ready(f, Duration.Inf)
      assert(res.isCompleted)
      assert(res.value.get.isSuccess)
    }
  }
}
