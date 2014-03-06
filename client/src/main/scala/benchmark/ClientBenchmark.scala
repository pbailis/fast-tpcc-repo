package edu.berkeley.velox.benchmark

import edu.berkeley.velox.conf.VeloxConfig
import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}
import scala.util.Random
import java.net.{Socket, InetSocketAddress}

import scala.util.{Success, Failure}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{Await, Future}
import scala.concurrent
import scala.concurrent.duration.Duration
import edu.berkeley.velox.benchmark.operation.{TPCCNewOrderRequest, TPCCNewOrderResponse}
import edu.berkeley.velox.benchmark.util.RandomGenerator
import java.util._
import java.util.concurrent.{LinkedBlockingQueue, Semaphore}
import edu.berkeley.velox.server.{SendStats, VeloxServer}
import java.nio.ByteBuffer
import com.typesafe.scalalogging.slf4j.Logging


object ClientBenchmark extends Logging {
  var totalWarehouses = -1
  val generator = new RandomGenerator

  val freeNumbers = new LinkedBlockingQueue[Int]()

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

    val clientChannel = new Socket
    clientChannel.connect(clusterAddresses(0))
    clientChannel.setTcpNoDelay(true)

    logger.error(s"clientchannel is $clientChannel")


    @volatile var finished = false


    val seqNo = new AtomicInteger()

    new Thread(new Runnable {
      override def run() {
        while (true) {
          logger.error(s"S ${SendStats.numSent} ${SendStats.bytesSent} R ${SendStats.numRecv} ${SendStats.bytesRecv} T ${SendStats.tryRecv} ${SendStats.tryBytesRecv}")
          Thread.sleep(1000)
        }
      }
    }).start()

    println(s"Starting $parallelism threads!")

    for(i <- 0 to numops) {
      freeNumbers.add(i)
    }

    for (i <- 0 to parallelism) {
      new Thread(new Runnable {
        val rand = new Random
        override def run() = {
          while (!finished) {
            val toSend = freeNumbers.poll()

            clientChannel.synchronized {
            val cchannel = clientChannel.getOutputStream
            val toSendBuf = ByteBuffer.allocate(8)
            toSendBuf.putInt(4)
            toSendBuf.putInt(toSend)
            cchannel.write(toSendBuf.array())
            SendStats.bytesSent.addAndGet(8)
            SendStats.numSent.incrementAndGet()
            cchannel.flush()
            }
          }
        }
      }).start()
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

  class ReaderThread(
      channel: Socket) extends Thread {
      override def run() {
        while(true) {

          val len = VeloxServer.getInt(channel)

          SendStats.tryRecv.incrementAndGet()
          SendStats.tryBytesRecv.addAndGet(len)


          var readBytes = 0
          val msgArr = new Array[Byte](len)
          while(readBytes != len) {
            readBytes += channel.getInputStream.read(msgArr, readBytes, len-readBytes)
          }
          assert(readBytes == len)

          SendStats.bytesRecv.addAndGet(len+4)
          SendStats.numRecv.incrementAndGet()


          freeNumbers.add(ByteBuffer.wrap(msgArr).getInt())
        }
      }
    }


}
