package edu.berkeley.velox.benchmark

import edu.berkeley.velox.conf.VeloxConfig
import java.util.concurrent.atomic.AtomicInteger
import edu.berkeley.velox.datamodel._
import edu.berkeley.velox.datamodel.Schema
import edu.berkeley.velox.datamodel.DataModelConverters._
import scala.util.Random
import edu.berkeley.velox.frontend.VeloxConnection
import java.net.InetSocketAddress
import scala.concurrent.Await
import scala.concurrent.duration.Duration

// this causes our futures to not thread
import edu.berkeley.velox.util.NonThreadedExecutionContext.context

import scala.util.{Success, Failure}
import java.util.UUID


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
      opt[Int]('b', "buffer_size") foreach {
        p => VeloxConfig.bufferSize = p
      } text("Size (in bytes) to make the network buffer")
      opt[Int]("sweep_time") foreach {
        p => VeloxConfig.sweepTime = p
      } text("Time the ArrayNetworkService send sweep thread should wait between sweeps")
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

    // (num reqs, avg)
    var currentLatency = (0, 0.0)

    def updateLatency(runtime: Long) = synchronized {
      val ttl = currentLatency._1 * currentLatency._2
      val nc = currentLatency._1 + 1
      currentLatency = (nc,(ttl+runtime)/nc)
    }

    val ostart = System.nanoTime

    val client = new VeloxConnection(clusterAddresses)
    val DB_NAME = "client-benchmark-db" + UUID.randomUUID().toString
    val TABLE_NAME = "table1"
    val ID_COL = "id"
    val STR_COL = "str"
    val dbf = client.createDatabase(DB_NAME)
    Await.ready(dbf, Duration.Inf)
    println("database successfully added")

    val db = dbf.value.get.get

    val tblf = db.createTable(TABLE_NAME, Schema.pkey(ID_COL).columns(ID_COL->INTEGER_TYPE, STR_COL->STRING_TYPE))
    Await.ready(tblf, Duration.Inf)
    println("table successfully added")

    val table = tblf.value.get.get


    println(s"Starting $parallelism threads!")

  for (i <- 0 to parallelism) {
    new Thread(new Runnable {
      val rand = new Random
      override def run() = {
        while (opsSent.get < numops) {
          if (rand.nextDouble() < pctReads) {
            val f = (client select STR_COL from table where ID_COL === rand.nextInt(keyrange)).execute
            val startTime =
              if (computeLatency) System.nanoTime
              else 0
            f onComplete {
              case Success(value) => {
                if (computeLatency)
                  updateLatency(System.nanoTime-startTime)
                numGets.incrementAndGet()
                opDone
              }
              case Failure(t) => println("An error has occured: " + t.getMessage)
            }
          } else {
            val key = rand.nextInt(keyrange)
            val f = (client insert (ID_COL->key, STR_COL->key.toString) into table).execute

            val startTime =
              if (computeLatency) System.nanoTime
              else 0
            f onComplete {
              case Success(value) => {
                if (computeLatency)
                  updateLatency(System.nanoTime-startTime)
                numPuts.incrementAndGet()
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


    if(status_time > 0) {
      new Thread(new Runnable {
        override def run() {
          while(opsDone.get() < numops) {
            Thread.sleep(status_time*1000)
            val curTime = (System.nanoTime-ostart).toDouble/nanospersec
            val curThru = (numGets.get()+numPuts.get()).toDouble/curTime
            println(s"STATUS @ ${curTime}s: $curThru ops/sec ($opsDone ops done)")
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
    if (computeLatency)
      println(s"Average latency ${currentLatency._2} milliseconds")
    System.exit(0)
  }

  def nextKey(rand: Random, keyrange: Int): String = {
    rand.nextInt(keyrange).toString
  }
}
