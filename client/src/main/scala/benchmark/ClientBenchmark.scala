package edu.berkeley.velox.benchmark

import edu.berkeley.velox.conf.VeloxConfig
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import edu.berkeley.velox.datamodel.Key
import edu.berkeley.velox.datamodel.Value
import scala.concurrent.Future
import scala.util.Random
import edu.berkeley.velox.frontend.VeloxConnection
import java.net.InetSocketAddress

import scala.util.{Success, Failure}
import scala.collection.JavaConversions._
// this causes our futures to not thread
import edu.berkeley.velox.util.NonThreadedExecutionContext.context

object ClientBenchmark {

  def main(args: Array[String]) {
    val numPuts = new AtomicInteger()
    val numGets = new AtomicInteger()

    val nanospersec = math.pow(10, 9)

    val keyrange = 10000
    var parallelism = 64
    var numops = 100000
    var waitTimeSeconds = 20
    var pctReads = 0.5
    var status_time = 10

    var useFutures = true
    var computeLatency = false

    var frontendCluster = ""

    val parser = new scopt.OptionParser[Unit]("velox") {
      opt[String]('m', "frontend_cluster") required() foreach {
        i => frontendCluster = i
      } text ("Frontend cluster")
      opt[Int]("timeout") foreach {
        i => waitTimeSeconds = i
      } text ("Time (s) for benchmark")
      opt[Int]("ops") foreach {
        i => numops = i
      } text ("Ops in the benchmark")
      opt[Int]("parallelism") foreach {
        i => parallelism = i
      } text ("Parallelism in thread pool")
      opt[Double]("pct_reads") foreach {
        i => pctReads = i
      } text ("Percentage read vs. write operations")
      opt[Int]("status_time") foreach {
        i => status_time = i
      }
      opt[Boolean]("usefutures") foreach {
        i => useFutures = i
      } text ("Use futures instead of blocking for reply")
      opt[Boolean]("latency") foreach {
        i => computeLatency = i
      } text ("Compute average latency of each request")
      opt[String]("network_service") foreach {
        i => VeloxConfig.networkService = i
      } text ("Which network service to use [nio/array]")
    }

    val opsSent = new AtomicInteger(0)
    val opsDone = new AtomicInteger(0)

    def opDone() {
      val o = opsDone.incrementAndGet
      if (o == numops) {
        opsDone.synchronized {
          opsDone.notify
        }
      }
    }

    parser.parse(args)

    val clusterAddresses = frontendCluster.split(",").map {
      a => val addr = a.split(":"); new InetSocketAddress(addr(0), addr(1).toInt)
    }

    val runningTimeMap =
      if (computeLatency)
        new ConcurrentHashMap[Future[Value],Long]()
      else null

    def computeRuntime(f: Future[Value]) {
      val time = System.nanoTime - runningTimeMap.get(f)
      runningTimeMap.put(f,time)
    }

    val ostart = System.nanoTime

    val client = new VeloxConnection(clusterAddresses)

    println(s"Starting $parallelism threads!")

    if (useFutures) {
      for (i <- 0 to parallelism) {
        new Thread(new Runnable {
          val rand = new Random
          override def run() = {
            while (opsSent.get < numops) {
              if (rand.nextDouble() < pctReads) {
                val f = client.getValueFuture(Key(rand.nextInt(keyrange)))
                if (computeLatency) runningTimeMap.put(f,System.nanoTime)
                f onComplete {
                  case Success(value) => {
                    numGets.incrementAndGet()
                    if (computeLatency) computeRuntime(f)
                    opDone
                  }
                  case Failure(t) => println("An error has occured: " + t.getMessage)
                }
              } else {
                val f = client.putValueFuture(Key(rand.nextInt(keyrange)), Value(rand.alphanumeric.take(10).toList.mkString))
                if (computeLatency) runningTimeMap.put(f,System.nanoTime)
                f onComplete {
                  case Success(value) => {
                    numPuts.incrementAndGet()
                    if (computeLatency) computeRuntime(f)
                    opDone
                  }
                  case Failure(t) => println("An error has occured: " + t.getMessage)
                }
              }
              opsSent.incrementAndGet
            }
          }
          println("Thread is done sending")
        }).start
      }
    } else {
      for (i <- 0 to parallelism) {
        new Thread(new Runnable {
          val rand = new Random
          override def run() = {
            while (true) {
              if (rand.nextDouble() < pctReads) {
                client.getValue(Key(rand.nextInt(keyrange)))
                numGets.incrementAndGet()
              } else {
                client.putValue(Key(rand.nextInt(keyrange)), Value(rand.alphanumeric.take(10).toList.mkString))
                numPuts.incrementAndGet()
              }

              if (opsDone.incrementAndGet() == numops) {
                opsDone.synchronized {
                  opsDone.notify
                }
              }
            }
          }
        }).start
      }
    }

    if(status_time > 0) {
      new Thread(new Runnable {
        override def run() {
          while(opsDone.get() < numops) {
            Thread.sleep(status_time*1000)
            val curTime = (System.nanoTime-ostart).toDouble/nanospersec
            val curThru = (numGets.get()+numPuts.get()).toDouble/curTime
            println(s"STATUS @ ${curTime}s: $curThru ops/sec ($opsDone.get ops done)")
          }
        }
      }).start
    }

    opsDone.synchronized {
      opsDone.wait(waitTimeSeconds * 1000)
    }

    val gstop = System.nanoTime
    val gtime = (gstop - ostart) / nanospersec

    val nPuts = numPuts.get()
    val nGets = numGets.get()

    val pthruput = nPuts.toDouble / gtime.toDouble
    val gthruput = nGets.toDouble / gtime.toDouble
    val totthruput = (nPuts + nGets).toDouble / gtime.toDouble
    println(s"In $gtime seconds and with $parallelism threads, completed $numPuts PUTs ($pthruput ops/sec), $numGets GETs ($gthruput ops/sec)\nTOTAL THROUGHPUT: $totthruput ops/sec")
    if (computeLatency) {
      runningTimeMap.values.foreach(v => {
        if (v > 30000000000000l) println("SOMEHTING BAD")
      })
      val total = runningTimeMap.values.filter(_<30000000000000l).foldLeft(0l)(_+_)
      val avg = (total/runningTimeMap.size.toDouble)/1000000.0
      println(s"Average latency $avg milliseconds")
    }
    System.exit(0)
  }

}
