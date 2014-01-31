package edu.berkeley.velox.benchmark

import java.util.concurrent.atomic.AtomicInteger
import edu.berkeley.velox.datamodel.Key
import edu.berkeley.velox.datamodel.Value
import scala.util.Random
import edu.berkeley.velox.frontend.VeloxConnection
import java.net.InetSocketAddress


object ClientBenchmark {

  def main(args: Array[String]) {
    val numPuts = new AtomicInteger()
    val numGets = new AtomicInteger()


    val rand = new Random
    val nanospersec = math.pow(10, 9)

    val keyrange = 10000
    var parallelism = 64
    var numops = 100000
    var waitTimeSeconds = 20
    var pctReads = 0.5
    var status_time = 10


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
    }

    val opsDone = new AtomicInteger(0)

    parser.parse(args)

    val clusterAddresses = frontendCluster.split(",").map {
      a => val addr = a.split(":"); new InetSocketAddress(addr(0), addr(1).toInt)
    }

    val ostart = System.nanoTime

    val client = new VeloxConnection(clusterAddresses)

    println(s"Starting $parallelism threads!")

    for (i <- 0 to parallelism) {
      new Thread(new Runnable {
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

    if(status_time > 0) {
      new Thread(new Runnable {
        override def run() {
          while(opsDone.get() < numops) {
            Thread.sleep(status_time*1000)
            val curTime = (System.nanoTime-ostart).toDouble/nanospersec
            val curThru = (numGets.get()+numPuts.get()).toDouble/curTime
            println(s"STATUS @ ${curTime}s: $curThru ops/sec")
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
    System.exit(0)
  }

}
