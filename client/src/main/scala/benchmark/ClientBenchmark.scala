package edu.berkeley.velox.benchmark

import java.util.concurrent.atomic.AtomicInteger
import edu.berkeley.velox.{Key, Value}
import edu.berkeley.velox.conf.VeloxConfig
import scala.util.Random
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit, ThreadPoolExecutor}
import edu.berkeley.velox.frontend.VeloxConnection


object ClientBenchmark {

  def main(args: Array[String]) {
    val numPuts = new AtomicInteger()
    val numGets = new AtomicInteger()

    VeloxConfig.initialize(args)

    val client = new VeloxConnection

    val rand = new Random
    val nanospersec = math.pow(10, 9)
    val numops = 1000000
    val keyrange = 10000
    val queuesize = 64
    val waitTime = 20

    // PUTS
    val putExecutor = new ThreadPoolExecutor(10, 50, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue[Runnable](queuesize))
    putExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy)
    //val putExecutor = Executors.newFixedThreadPool(30)

    println("Starting puts")
    //start timing
    val pstart = System.nanoTime
    for (i <- 0 to numops) {
      putExecutor.execute(new Runnable {
        def run = {
          client.putValue(Key(rand.nextInt(keyrange)), Value(rand.alphanumeric.take(10).toList.mkString))
          //numPuts.incrementAndGet()
          if (i % 1000 == 0) {
            println(s"finished put $i")
          }
        }
      })
    }
    putExecutor.shutdown()
    if(putExecutor.awaitTermination(waitTime, TimeUnit.SECONDS)) {
      val pstop = System.nanoTime
      val ptime = (pstop - pstart) / nanospersec
      val pthruput = numops.toDouble/ptime.toDouble
      println(s"Completed $numops PUTS in $ptime seconds - THROUGHPUT: $pthruput")
    } else {
      println(s"putExecutor shutdown incomplete after $waitTime seconds")
      return
    }

    // GETS
    val getExecutor = new ThreadPoolExecutor(10, 50, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue[Runnable](queuesize))
    getExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy)

    println("starting gets")
    val gstart = System.nanoTime
    for (i <- 0 to numops) {

      getExecutor.execute(new Runnable {
        def run = {
          client.getValue(Key(rand.nextInt(keyrange)))
          //numGets.incrementAndGet()
          if (i % 100000 == 0) {
            println(s"finished get $i")
          }
        }
      })
    }

    getExecutor.shutdown()
    if(getExecutor.awaitTermination(waitTime, TimeUnit.SECONDS)) {
      val gstop = System.nanoTime
      val gtime = (gstop - gstart) / nanospersec
      val gthruput = numops.toDouble/gtime.toDouble
      println(s"Completed $numops GETS in $gtime seconds - THROUGHPUT: $gthruput")
    } else {
    println(s"getExecutor shutdown incomplete after $waitTime seconds")
    }

  }

}
