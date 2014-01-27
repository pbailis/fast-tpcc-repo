package edu.berkeley.velox.benchmark.nio

import edu.berkeley.velox.conf.VeloxConfig
import edu.berkeley.velox.net.{NetworkService, NIONetworkService}
import edu.berkeley.velox.rpc.{InternalRPCService, MessageService}
import java.util.concurrent.atomic.AtomicInteger
import edu.berkeley.velox.NetworkDestinationHandle
import java.util.concurrent.Executors


class PingMessageService(val totalPing: Int,
                         val sendThreads: Int,
                         val receiveThreads: Int,
                         val msgSize: Int) extends InternalRPCService {
  val pings = new AtomicInteger(0)
  val sendExecutor = Executors.newFixedThreadPool(sendThreads)
  var startTime: Long = 0L

  def sendPings(partitionId: NetworkDestinationHandle) {
    startTime = System.currentTimeMillis()
    val bytes = new Array[Byte](msgSize)
    for (i <- 0 until 10) {
      sendExecutor.execute(new Runnable {
        def run() {
          for (j <- 0 until totalPing / 10) {
            PingMessageService.this.networkService.send(partitionId, bytes)
          }
        }
      })
    }
  }

  override def receiveRemoteMessage(src: NetworkDestinationHandle, bytes: Array[Byte]): Unit = {
    val count = pings.incrementAndGet()
    if (count == totalPing) {
      val endTime = System.currentTimeMillis()
      val startTime = PingMessageService.this.startTime
      val msgsSent = PingMessageService.this.totalPing
      val msgSize = PingMessageService.this.msgSize
      val elapsedTime = (endTime - startTime).toDouble / 1000.0
      val mbSent = msgsSent.toDouble * msgSize / 1048576.0
      println(s"Finished in ${elapsedTime} seconds.")
      println(s"Ping-Pong rate: ${msgsSent.toDouble / elapsedTime}")
      println(s"Data rate: ${mbSent / elapsedTime} (MB/Sec)")
      val actualBytesRecv = networkService.bytesReceivedMeter.getCount
      println(s"Physical Mbytes received: ${actualBytesRecv / 1048576.0}")
      println(s"Physical transfer rate: ${actualBytesRecv / (1048576.0 * elapsedTime)} (MB/Sec)")

    }
  }
}

class PingPongMessageService(val totalPingPongs: Int,
                             val sendThreads: Int,
                             val receiveThreads: Int,
                             val msgSize: Int) extends InternalRPCService {
  override val name = "pong"
  val pingPongs = new AtomicInteger(0)
  val sendExecutor = Executors.newFixedThreadPool(sendThreads)
  val receiveExecutor = Executors.newFixedThreadPool(receiveThreads)
  var startTime: Long = 0L


  def sendPings(partitionId: NetworkDestinationHandle) {
    startTime = System.currentTimeMillis()
    val bytes = new Array[Byte](msgSize)
    val ns = networkService
    for (i <- 0 until 10) {
      sendExecutor.execute(new Runnable {
        def run() {
          for (j <- 0 until totalPingPongs / 10) {
            ns.send(partitionId, bytes)
          }
        }
      })
    }
  }

  override def receiveRemoteMessage(src: NetworkDestinationHandle, bytes: Array[Byte]): Unit = {
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
        val actualBytesWritten = networkService.bytesWrittenMeter.getCount
        println(s"Physical Mbytes transfered: ${actualBytesWritten / 1048576.0}")
        println(s"Physical transfer rate: ${actualBytesWritten / (1048576.0 * elapsedTime)} (MB/Sec)")

      }
    } else {
      // This is a ping message
//      receiveExecutor.execute( new Runnable {
//        def run() {
          val ns: NetworkService = PingPongMessageService.this.networkService
          // make bytes a pong message
          bytes(0) = 3
          ns.send(src, bytes)
//        }
//      })
    }
  }
}


object NetworkServiceBenchmark {
  def main(args: Array[String]) {
    // Parse command line and setup environment
    VeloxConfig.initialize(args)
    println(s"Starting node ${VeloxConfig.partitionId} ")
    Thread.sleep(1000 * 5)  // yourkit timing delay
    val totalPingPongs = 5000000
    val sendThreads = 4
    val receiveThreads = 4
    val msgSize = 64

    val ms = new PingPongMessageService(totalPingPongs, sendThreads, receiveThreads, msgSize)
    ms.initialize()

    //val frontendServer = new PingMessageService(totalPingPongs, sendThreads, receiveThreads, msgSize)



    val other = (VeloxConfig.partitionId+1) % VeloxConfig.partitionList.size
    ms.sendPings(other)

  }
}
