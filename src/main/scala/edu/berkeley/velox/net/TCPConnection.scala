package edu.berkeley.velox.net

import java.net.{Socket, InetSocketAddress}
import java.util.concurrent.LinkedBlockingQueue
import java.io.{DataInputStream, DataOutputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer
import edu.berkeley.velox.PartitionId
import java.nio.channels.{Channels, SocketChannel}
import edu.berkeley.velox.conf.VeloxConfig

/**
 *  For now, use producer-consumer model where connection
 *  only knows about how to send/recv bytes.
 *
 *  Uses (non-nio) sockets with RLE for messages.
 */

class TCPConnection(val clientSocket: Socket,
                    val netService: NetworkService,
                    var connectedPartition: PartitionId = -1) {
  val sendQueue = new LinkedBlockingQueue[Array[Byte]]()
  val dataInputStream = new DataInputStream(clientSocket.getInputStream)
  val dataOutputStream = new DataOutputStream(clientSocket.getOutputStream)

  def start() {
    // writing thread
    new Thread(new Runnable {
      def run {
        while (true) {
          val toSend = sendQueue.take()

          dataOutputStream.writeInt(toSend.length)
          dataOutputStream.write(toSend)
          dataOutputStream.flush()
        }
      }
    }).start()

    // reading thread
    new Thread(new Runnable {

      def run {
        while (true) {
          val msgLen = dataInputStream.readInt()
          val msgBuf = new Array[Byte](msgLen)
          dataInputStream.read(msgBuf, 0, msgLen)
          netService.recv(connectedPartition, msgBuf)
        }
      }
    }).start()
  }

  def close() {
    clientSocket.close
  }

  def send(msgBytes: Array[Byte]) {
    sendQueue.add(msgBytes)
  }
}