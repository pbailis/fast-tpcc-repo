package edu.berkeley.velox.server

import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox._
import edu.berkeley.velox.cluster.{TPCCPartitioner}
import edu.berkeley.velox.conf.VeloxConfig
import edu.berkeley.velox.rpc.{FrontendRPCService, InternalRPCService, MessageHandler, Request}
import java.util.concurrent.ConcurrentHashMap
import scala.concurrent.{Future, future}
import edu.berkeley.velox.storage.StorageEngine
import edu.berkeley.velox.benchmark.operation._
import edu.berkeley.benchmark.tpcc.TPCCNewOrder
import edu.berkeley.velox.benchmark.datamodel.serializable.{SerializableRow, LockManager}
import java.util
import edu.berkeley.velox.datamodel.{Row, PrimaryKey}
import edu.berkeley.velox.benchmark.operation.serializable.TPCCNewOrderSerializable
import java.util.Collections

import edu.berkeley.velox.util.NonThreadedExecutionContext.context
import java.net.{Socket, ServerSocket}
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong


object VeloxServer extends Logging {
  def main(args: Array[String]) {
    logger.info("Initializing Server")
    VeloxConfig.initialize(args)

    new Thread(new Runnable {
      override def run() {
        while (true) {
          logger.error(s"S ${SendStats.numSent} ${SendStats.bytesSent} R ${SendStats.numRecv} ${SendStats.bytesRecv} T ${SendStats.tryRecv} ${SendStats.tryBytesRecv}")
          Thread.sleep(1000)
        }
      }
    }).start()

    // initialize network service and message service
    val serverChannel = new ServerSocket(VeloxConfig.externalServerPort)

    while (true) {
      // Accept the client socket
      val clientChannel = serverChannel.accept()
      logger.error(s"got connection!")
      clientChannel.setTcpNoDelay(true)
      // Get the bytes encoding the source partition Id
      new ServerHandlerThread(clientChannel).start()

    }


  }



  def getInt(socket: Socket): Int =  {
     val intArr = new Array[Byte](4)
     var read = 0
     val input = socket.getInputStream
     while(read != 4) {
       read = input.read(intArr, read, 4)
     }
     ByteBuffer.wrap(intArr).getInt()
   }

   def writeInt(socket: Socket, i: Int) {
     val intArr = ByteBuffer.wrap(new Array[Byte](4)).putInt(i).array()
     socket.getOutputStream.write(intArr)
     socket.getOutputStream.flush()
   }
}


class ServerHandlerThread(
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


      val cos = channel.getOutputStream
      cos.write(len)
      cos.write(msgArr)
      cos.flush()

      SendStats.bytesSent.addAndGet(4+msgArr.size)
      SendStats.numSent.incrementAndGet()
    }
  }
}

object SendStats {
  val numSent = new AtomicLong
  val bytesSent = new AtomicLong
  val numRecv = new AtomicLong
  val bytesRecv = new AtomicLong
  val tryRecv = new AtomicLong
  val tryBytesRecv = new AtomicLong

}
