package edu.berkeley.velox.benchmark

import edu.berkeley.velox.conf.VeloxConfig
import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}
import scala.util.Random
import edu.berkeley.velox.frontend.VeloxConnection
import java.net.InetSocketAddress

import scala.util.{Success, Failure}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent
import scala.concurrent.duration.Duration
import edu.berkeley.velox.benchmark.operation.{TPCCNewOrderRequest, TPCCNewOrderResponse}
import edu.berkeley.velox.benchmark.util.RandomGenerator
import java.util._
import java.util.concurrent.Semaphore
import java.util.HashMap


object ClientBenchmark {
  var totalWarehouses = -1
  val generator = new RandomGenerator


  def main(args: Array[String]) {
    val numAborts = new AtomicInteger()
    val numMs = new AtomicLong()

    val nanospersec = math.pow(10, 9)

    val keyrange = 10000
    var parallelism = 64
    var numops = 100000
    var waitTimeSeconds = 20
    var chance_remote = 0.01
    var status_time = 10
    var warehouses_per_server = 1
    var load = false
    var run = false
    var serializable = false
    var pct_test = false
    var connection_parallelism = 1

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
      opt[Double]("chance_remote") foreach {
        i => chance_remote = i
      } text ("Percentage read vs. write operations")
      opt[Int]("status_time") foreach {
        i => status_time = i
      }
      opt[Boolean]("tcp_nodelay") foreach {
        i => VeloxConfig.tcpNoDelay = i
      }
      opt[Int]("connection_parallelism") foreach {
        i => connection_parallelism = i
      }
      opt[Int]('b', "buffer_size") foreach {
        p => VeloxConfig.bufferSize = p
      } text("Size (in bytes) to make the network buffer")
      opt[Int]("sweep_time") foreach {
        p => VeloxConfig.sweepTime = p
      } text("Time the ArrayNetworkService send sweep thread should wait between sweeps")

      opt[Int]("warehouses_per_server") foreach {
        i => warehouses_per_server = i
      }
      opt[Unit]("load") foreach { p => load = true }
      opt[Unit]("run") foreach { p => run = true }
      opt[Unit]("serializable") foreach { p => serializable = true }
      opt[Unit]("pct_test") foreach { p => pct_test = true }



      opt[String]("network_service") foreach {
        i => VeloxConfig.networkService = i
      } text ("Which network service to use [nio/array]")
    }

    val opsDone = new AtomicInteger(0)

    parser.parse(args)

    val clusterAddresses = frontendCluster.split(",").map {
      a => val addr = a.split(":"); new InetSocketAddress(addr(0), addr(1).toInt)
    }

    var client: VeloxConnection = null

    totalWarehouses = clusterAddresses.size*warehouses_per_server

    if(load) {
      client = new VeloxConnection(clusterAddresses, connection_parallelism, null)
      totalWarehouses = clusterAddresses.size*warehouses_per_server
      println(s"Loading $totalWarehouses warehouses...}")
      val loadFuture = Future.sequence((1 to totalWarehouses).map(wh => client.loadTPCC(wh)))
      Await.result(loadFuture, Duration.Inf)
      println(s"...loaded ${totalWarehouses} warehouses")
    }

    if(!run) {
      System.exit(0)
    }

    val prunedClusterAddresses = new Array[InetSocketAddress](8)
    val our_warehouses = new Array[Int](8)
    val whToServer = new java.util.HashMap[Int, Int]

    for(i <- 0 to 7) {
      var picked = false
      while(!picked) {
        val random_idx = Math.abs(Random.nextInt()) % clusterAddresses.size
        val randomChoice = clusterAddresses(random_idx)
        if(randomChoice != null) {
          prunedClusterAddresses(i) = randomChoice
          clusterAddresses(random_idx) = null
          picked = true
          our_warehouses(i) = random_idx
          whToServer.put(random_idx, i)
        }
      }
    }

    client = new VeloxConnection(prunedClusterAddresses, connection_parallelism, whToServer)

    @volatile var finished = false
    val requestSem = new Semaphore(0)


    println(s"Starting $parallelism threads!")

    for (i <- 0 to parallelism) {
      new Thread(new Runnable {
        val rand = new Random
        override def run() = {
          while (!finished) {
            requestSem.acquire()

            val request = singleNewOrder(client, chance_remote, our_warehouses, serializable, pct_test)
            request.future onComplete {
              case Success(value) => {
                numMs.addAndGet(System.currentTimeMillis()-request.startTimeMs)
                if(!value.committed) {
                 numAborts.incrementAndGet()
                }

                opsDone.incrementAndGet
                requestSem.release
              }
              case Failure(t) => println("An error has occured: " + t.getMessage)
            }
          }
        }
      }).start
    }

    if(status_time > 0) {
      new Thread(new Runnable {
        override def run() {
          val tstart = System.nanoTime
          while(!finished) {
            Thread.sleep(status_time*1000)
            val curTime = (System.nanoTime-tstart).toDouble/nanospersec
            val curThru = (opsDone.get()).toDouble/curTime
            val latency = numMs.get()/opsDone.get().toDouble

            println(s"STATUS @ ${curTime}s: $curThru ops/sec (lat: $latency ms)")
          }
        }
      }).start
    }

    requestSem.release(numops)
    val ostart = System.nanoTime

    Thread.sleep(waitTimeSeconds * 1000)
    finished = true

    val gstop = System.nanoTime
    val gtime = (gstop - ostart) / nanospersec

    val nOps = opsDone.get()
    val latency = numMs.get()/opsDone.get().toDouble

    val pthruput = nOps.toDouble / gtime.toDouble
    println(s"In $gtime seconds and with $parallelism threads, completed $opsDone, $numAborts aborts \nTOTAL THROUGHPUT: $pthruput ops/sec (avg latency ${latency} ms)")
    System.exit(0)
  }

  def singleNewOrder(conn: VeloxConnection, chance_remote: Double, our_warehouses: Array[Int], serializable: Boolean = false, pct_test: Boolean = false): OutstandingNewOrderRequest = {
    val W_ID = our_warehouses(Math.abs(Random.nextInt()) % our_warehouses.size)
    val D_ID: Int = generator.number(1, 10)
    val C_ID: Int = generator.NURand(1023, 1, 3000)
    val OL_CNT: Int = generator.number(5, 15)
    val rollback: Boolean = generator.nextDouble < .01
    var warehouseIDs = new ArrayList[Int]()

    for(i <- 1 to OL_CNT) {
      var O_W_ID = W_ID
      if (totalWarehouses > 1) {
        if(!pct_test && generator.nextDouble() < chance_remote) {
          O_W_ID = generator.numberExcluding(1, totalWarehouses, W_ID)

        }
        // in the remote tests, we want to precisely control the number of remote txns, so we
        // only make the first item have a remote id
        else if(pct_test && i == 1 && generator.nextDouble() < chance_remote) {
          O_W_ID = generator.numberExcluding(1, totalWarehouses, W_ID)
        }
      }

      warehouseIDs.add(O_W_ID)
    }

    var OL_I_IDs = new ArrayList[Int]()
    var OL_QUANTITY_LIST = new ArrayList[Int]()

    for(ol_cnt <- 1 to OL_CNT) {
      var OL_I_ID = generator.NURand(8191, 1, 100000)
      if(rollback && ol_cnt == OL_CNT) {
        OL_I_ID = -1
      }

      OL_I_IDs.add(OL_I_ID)
      OL_QUANTITY_LIST.add(generator.number(1, 10))
    }

    return new OutstandingNewOrderRequest(conn.newOrder(new TPCCNewOrderRequest(W_ID, D_ID, C_ID, OL_I_IDs, warehouseIDs, OL_QUANTITY_LIST, serializable)),
                                          System.currentTimeMillis())
  }

  case class OutstandingNewOrderRequest(future: Future[TPCCNewOrderResponse], startTimeMs: Long)

}
