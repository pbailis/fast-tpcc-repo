package edu.berkeley.velox.net

import edu.berkeley.velox.rpc.{MessageService, KryoMessageService, Request}
import java.net.{ServerSocket, Socket, InetSocketAddress}
import edu.berkeley.velox.PartitionId
import java.nio.channels.{SocketChannel, ServerSocketChannel}
import edu.berkeley.velox.conf.VeloxConfig
import java.io.{DataOutputStream, DataInputStream}
import java.util.concurrent.ConcurrentHashMap


class BasicNetworkService extends NetworkService {
  var connections = new ConcurrentHashMap[PartitionId, TCPConnection]
  var initialized = false

  def start() {
    this.synchronized {
      if (initialized) {
        // TODO: error
        println("Already initialized NetworkService!")
        return
      }
    }

    initialized = true

    val serverSocket = new ServerSocket(VeloxConfig.serverPort)
    val ns = this

    new Thread(new Runnable {
      def run() = {
        while (true) {
          val clientSocket = serverSocket.accept
          val tcp = new TCPConnection(clientSocket, ns)

          val remotePartitionId = tcp.dataInputStream.readInt()
          tcp.connectedPartition = remotePartitionId
          tcp.start()

          if (connections.putIfAbsent(remotePartitionId, tcp) != null) {
            println("Already connected to " + remotePartitionId)
            tcp.close
          }
        }
      }
    }).start()

    Thread.sleep(VeloxConfig.bootstrapConnectionWaitSeconds * 1000)

    // connect to all higher-numbered partitions
    VeloxConfig.serverAddresses.filter {
      case (id, addr) => id > VeloxConfig.partitionId
    }.foreach {
      case (remoteId, remoteAddress) =>
        val remoteSocket = new Socket(remoteAddress.getHostName, remoteAddress.getPort)

        remoteSocket.setTcpNoDelay(VeloxConfig.tcpNoDelay)

        val tcp = new TCPConnection(remoteSocket, ns, remoteId)
        tcp.dataOutputStream.writeInt(VeloxConfig.partitionId)
        tcp.dataOutputStream.flush()
        tcp.start()

        if (connections.putIfAbsent(remoteId, tcp) != null) {
          println("Already connected to " + remoteId)
          tcp.close
        }
    }

    Thread.sleep(VeloxConfig.bootstrapConnectionWaitSeconds * 1000)
  }

  override def setMessageService(messageService: MessageService) {
    this.messageService = messageService
    this.messageService.networkService = this
  }

  def send(dst: PartitionId, buffer: Array[Byte]) {
    connections.get(dst).send(buffer)
  }

  def recv(src: PartitionId, buffer: Array[Byte]) {
    messageService.receiveRemoteMessage(src, buffer)
  }
}