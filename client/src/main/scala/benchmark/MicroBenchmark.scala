package edu.berkeley.velox.benchmark

import edu.berkeley.velox.conf.VeloxConfig
import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}
import scala.util.Random
import edu.berkeley.velox.frontend.VeloxConnection
import java.net.InetSocketAddress

import scala.util.{Success, Failure}
import edu.berkeley.velox.util.NonThreadedExecutionContext.context
import scala.concurrent.{Await, Future}
import scala.concurrent
import scala.concurrent.duration.Duration
import edu.berkeley.velox.benchmark.operation._
import edu.berkeley.velox.benchmark.util.RandomGenerator
import java.util._
import java.util.concurrent.Semaphore
import scala.util.Failure
import scala.util.Success
import scala.collection.JavaConverters._


object MicroBenchmark {
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
    var serializable = false
    var connection_parallelism = 1


    var cfree = false
    var twopl = false
    var opt_twopl = false
    var num_items = 1

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
      opt[Int]("connection_parallelism") foreach {
        i => connection_parallelism = i
      }
      opt[Int]('b', "buffer_size") foreach {
        p => VeloxConfig.bufferSize = p
      } text("Size (in bytes) to make the network buffer")
      opt[Int]("sweep_time") foreach {
        p => VeloxConfig.sweepTime = p
      } text("Time the ArrayNetworkService send sweep thread should wait between sweeps")


      opt[Unit]("cfree") foreach( p=> cfree = true )
      opt[Unit]("twopl") foreach( p=> twopl = true )
      opt[Unit]("opt_twopl") foreach( p=> opt_twopl = true )



      opt[Int]("num_items") foreach {
        i => num_items = i
      }
      opt[Unit]("serializable") foreach { p => serializable = true }


      opt[String]("network_service") foreach {
        i => VeloxConfig.networkService = i
      } text ("Which network service to use [nio/array]")
    }



    val opsDone = new AtomicInteger(0)

    parser.parse(args)

    val clusterAddresses = frontendCluster.split(",").map {
      a => val addr = a.split(":"); new InetSocketAddress(addr(0), addr(1).toInt)
    }

    val client = new VeloxConnection(clusterAddresses, connection_parallelism)

    @volatile var finished = false
    val requestSem = new Semaphore(0)


    val numServers = clusterAddresses.size

    println(s"Starting $parallelism threads!")

    for (i <- 0 to parallelism) {
      new Thread(new Runnable {
        val rand = new Random
        override def run() = {
          while (!finished) {
            requestSem.acquireUninterruptibly()

            val st = System.currentTimeMillis()

            if(cfree) {
              val startServer = Math.abs(Random.nextInt)
              var i = 0
              var futures = new ArrayList[Future[Boolean]](num_items)
              while(i < num_items) {
                futures.add(client.send(((startServer+i) % numServers)+1, new MicroCfreePut))
                i += 1
              }

              val combinedFuture = Future.sequence(futures.asScala)
              combinedFuture.onComplete {
                case Success(value) => {
                  opsDone.incrementAndGet()
                  requestSem.release
                  numMs.addAndGet(System.currentTimeMillis()-st)
                }
                case Failure(t) => println("An error has occurred: "+t.getMessage)
              }
            } else if (twopl) {
              // to avoid deadlocks, we never wrap around
              val startServer: Int = (Math.abs(Random.nextInt) % (numServers-num_items+1))
              var i = 0
              while(i < num_items) {
                val f = client.send(startServer+i+1, new MicroTwoPLPutAndLock)
                Await.ready(f, Duration.Inf)
                i += 1
              }

              i = 0
              while(i < num_items) {
                client.send(startServer+i+1, new MicroTwoPLUnlock)
                i += 1
              }
              opsDone.incrementAndGet()
              numMs.addAndGet(System.currentTimeMillis()-st)
              requestSem.release
            } else if(opt_twopl) {
              // to avoid deadlocks, we never wrap around
              val startServer: Int = (Math.abs(Random.nextInt) % (numServers-num_items+1))
              val future = client.send((startServer)+1, new MicroOptimizedTwoPL(VeloxConfig.partitionId, -1, num_items, num_items))

              future.onComplete {
                case Success(value) => {
                    opsDone.incrementAndGet()
                    requestSem.release
                    numMs.addAndGet(System.currentTimeMillis()-st)

                  }
                  case Failure(t) => println("An error has occurred: "+t.getMessage)
              }
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

            println(s"STATUS @ ${curTime}s: $curThru ops/sec (lat: $latency ms; $opsDone")
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
}
