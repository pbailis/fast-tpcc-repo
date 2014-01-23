package edu.berkeley.velox.benchmark.nio

import edu.berkeley.velox.conf.VeloxConfig
import edu.berkeley.velox.net.{NetworkService, NIONetworkService}
import edu.berkeley.velox.rpc.{MessageService, MessageHandler, Request, KryoMessageService}
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}
import scala.concurrent.duration.Duration
import java.util.concurrent.atomic.AtomicInteger
import edu.berkeley.velox.PartitionId
import java.util.concurrent.Executors


class PingPongMessageService(val totalPingPongs: Int,
                             val sendThreads: Int,
                             val receiveThreads: Int,
                             val msgSize: Int) extends MessageService {
  val pingPongs = new AtomicInteger(0)
  val sendExecutor = Executors.newFixedThreadPool(sendThreads)
  val receiveExecutor = Executors.newFixedThreadPool(receiveThreads)
  var startTime: Long = 0L

  override def setNetworkService(networkService: NetworkService): Unit =  {
    this.networkService = networkService
    this.networkService.messageService = this
  }

  def sendPings(partitionId: PartitionId) {
    startTime = System.currentTimeMillis()
    val bytes = new Array[Byte](msgSize)
    for (i <- 0 until totalPingPongs) {
      sendExecutor.execute(new Runnable {
        def run() {
          PingPongMessageService.this.networkService.send(partitionId, bytes)
        }
      })
    }
  }

  override def receiveRemoteMessage(src: PartitionId, bytes: Array[Byte]): Unit = {
    val isPong = bytes(0) == 3
    if (isPong) {
      val count = pingPongs.incrementAndGet()
      if (count == totalPingPongs) {
        val endTime = System.currentTimeMillis()
        val startTime = PingPongMessageService.this.startTime
        val msgsSent = PingPongMessageService.this.totalPingPongs
        val msgSize = PingPongMessageService.this.msgSize
        val elapsedTime = (endTime - startTime).toDouble / 1000.0
        val mbSent = msgsSent.toDouble * msgSize / 1048576.0
        println(s"Finished in ${elapsedTime} seconds.")
        println(s"Ping-Pong rate: ${msgsSent.toDouble / elapsedTime}")
        println(s"Data rate: ${mbSent / elapsedTime} (MB/Sec)")
        val actualBytesWritten = networkService.bytesWrittenCounter.get()
        println(s"Physical Mbytes transfered: ${actualBytesWritten / 1048576.0}")
        println(s"Physical transfer rate: ${actualBytesWritten / (1048576.0 * elapsedTime)} (MB/Sec)")

      }
    } else {
      // This is a ping message
      receiveExecutor.execute( new Runnable {
        def run() {
          val ns: NetworkService = PingPongMessageService.this.networkService
          // make bytes a pong message
          bytes(0) = 3
          ns.send(src, bytes)
        }
      })
    }
  }
}

object NetworkServiceBenchmark {
  def main(args: Array[String]) {
    // Parse command line and setup environment
    VeloxConfig.initialize(args)
    println(s"Starting node ${VeloxConfig.partitionId} ")
    //Thread.sleep(1000 * 10)  // yourkit timing delay
    val totalPingPongs = 4000000
    val sendThreads = 8
    val receiveThreads = 8
    val msgSize = 64

    //val ns = new NetworkService
    val ms = new PingPongMessageService(totalPingPongs, sendThreads, receiveThreads, msgSize)
    val ns = new NIONetworkService
    ns.setMessageService(ms)
    ns.start()

    val other = (VeloxConfig.partitionId+1) % VeloxConfig.partitionList.size
    ms.sendPings(other)

  }
}
