package edu.berkeley.velox.net

import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.NetworkDestinationHandle
import edu.berkeley.velox.rpc.MessageService
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{ServerSocketChannel, SocketChannel}
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors, Semaphore}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.util.Random

object HackConfig {
  val bufSize = 500
}

class SocketBuffer(channel: SocketChannel) extends Runnable {
  var buf = new Array[Byte](HackConfig.bufSize)

  val writePos = new AtomicInteger(4)

  val writers = new AtomicInteger(0)
  val sending = new AtomicBoolean(false)

  private def writeInt(i: Int, offset: Int) {
    buf(offset)   = (i >>> 24).toByte
    buf(offset+1) = (i >>> 16).toByte
    buf(offset+2) = (i >>> 8).toByte
    buf(offset+3) = i.toByte
  }

  def write(bytes: Array[Byte]): Boolean = {
    // indicate our intent to write
    writers.incrementAndGet

    // fail if we're already sending this buffer
    if (sending.get) {
      writers.decrementAndGet
      return false
    }

    val len = bytes.length+4

    val writeOffset = writePos.getAndAdd(len)
    val ret =
      if (writeOffset + len <= buf.length) {
        writeInt(bytes.length,writeOffset)
        Array.copy(bytes,0,buf,writeOffset+4,bytes.length)
        true
      }
      else {
        writePos.getAndAdd(-len)
        false
      }

    //println(this+" at end: "+writePos.get)

    writers.decrementAndGet
    ret
  }

  /**
    * Calling this class as a runnable sends the current data
    *
    * sending MUST be set to true before asking this class to run
    */
  def run() {

    // wait for any writers to finish
    while (writers.get > 0) Thread.sleep(1)

    // write out full message length (minus 4 for these bytes)
    writeInt(writePos.get-4,0)

    // wrap the array and write it out
    //println("trying to send: "+writePos.get)
    //println("array: "+java.util.Arrays.toString(buf))
    val wrote = channel.write(ByteBuffer.wrap(buf,0,writePos.get))
    //println(s"send: ${wrote}")

    // reset write position
    writePos.set(4)

    sending.set(false)
  }

}

object IntReader {
  def readInt(array: Array[Byte], offset: Int=0):Int = {
    (array(offset) << 24) + ((array(offset+1) & 0xFF) << 16) + ((array(offset+2) & 0xFF) << 8) + (array(offset+3) & 0xFF)
  }
}

class Receiver(
  bytes: ByteBuffer,
  src: NetworkDestinationHandle,
  messageService: MessageService) extends Runnable {
  def run() {
    val total = bytes.getInt
    assert(bytes.hasArray)
    val theArray = bytes.array
    var curPos = 0
    //println("full array: "+java.util.Arrays.toString(theArray))
    while(bytes.remaining != 0) {
      val msgLen = IntReader.readInt(theArray,curPos)
      curPos+=4
      //println("msgLen: "+msgLen)
      val subarray = theArray.slice(curPos,curPos+msgLen)
      //println("subarray: "+java.util.Arrays.toString(subarray))
      messageService.receiveRemoteMessage(src,subarray)
      curPos+=msgLen
    }
    // TODO: Hand off message
    //executor.submit(new Sender(channel,reqs))
  }
}

class ReaderThread(
  channel: SocketChannel,
  executor: ExecutorService,
  src: NetworkDestinationHandle,
  messageService: MessageService) extends Thread {
  val sizeBuffer = ByteBuffer.allocate(4)
  override def run() {
    while(true) {
      channel.read(sizeBuffer)
      sizeBuffer.flip
      val len = IntReader.readInt(sizeBuffer.array)
      //println(s"message len to read: $len")
      sizeBuffer.rewind
      val readBuffer = ByteBuffer.allocate(len)
      channel.read(readBuffer)
      readBuffer.flip
      //println("read: "+java.util.Arrays.toString(readBuffer.array))
      executor.submit(new Receiver(readBuffer,src,messageService))
    }
  }
}

class SendSweeper(
  connections: ConcurrentHashMap[NetworkDestinationHandle, SocketBuffer],
  executor: ExecutorService) extends Runnable {

  def run() {
    while(true) {
      val cit = connections.keySet.iterator
      while (cit.hasNext) {
        val buf = connections.get(cit.next)
        if (buf.writePos.get > 4 && buf.sending.compareAndSet(false,true))
          executor.submit(buf)
      }
      Thread.sleep(10)
    }
  }
}

class ArrayNetworkService(
  val performIDHandshake: Boolean = false,
  val tcpNoDelay: Boolean = true,
  val serverID: Integer = -1) extends NetworkService with Logging {

  val executor = Executors.newCachedThreadPool()
  val connections = new ConcurrentHashMap[NetworkDestinationHandle, SocketBuffer]
  val nextConnectionID = new AtomicInteger(0)
  private val connectionSemaphore = new Semaphore(0)

  override def setMessageService(messageService: MessageService) {
    this.messageService = messageService
  }

  def blockForConnections(numConnections: Integer) {
    connectionSemaphore.acquireUninterruptibly(numConnections)
  }

  def start() {
    new Thread(new SendSweeper(connections,executor)).start
  }

  override def connect(handle: NetworkDestinationHandle, address: InetSocketAddress) {
    while(true) {
      try {
        val clientChannel = SocketChannel.open()
        clientChannel.connect(address)
        assert(clientChannel.isConnected)
        val bos = new ByteArrayOutputStream()
        val dos = new DataOutputStream(bos)

        if(performIDHandshake) {
          dos.writeInt(serverID)
          dos.flush()
          val bytes = bos.toByteArray()
          assert(bytes.size == 4)
          clientChannel.socket.getOutputStream.write(bytes)
        }
        _registerConnection(handle, clientChannel)
        return;
      } catch {
        case e: Exception => logger.error("Error connecting to "+address, e)
        Thread.sleep(500)
      }
    }
  }

  override def connect(address: InetSocketAddress): NetworkDestinationHandle = {
    val handle = nextConnectionID.incrementAndGet()
    connect(handle, address)
    handle
  }

  /**
    * Register a channel to be managed by this service
    *
    * This will start a thread to read from the channel
    * and allow sends to this channel through the service
    */
  def _registerConnection(partitionId: NetworkDestinationHandle, channel: SocketChannel) {
    val buf = new SocketBuffer(channel)
    if (connections.putIfAbsent(partitionId,buf) == null) {
      logger.info(s"Adding connection from $partitionId")
      // start up a read thread
      new ReaderThread(channel,executor,partitionId,messageService).start
      connectionSemaphore.release
    }
  }

  override def configureInboundListener(port: Integer) {
    val serverChannel = ServerSocketChannel.open()
    logger.info("Listening on: "+port)
    serverChannel.socket.bind(new InetSocketAddress(port))

    new Thread {
      override def run() {
        // Grab references to memebers in the parent class
        val connections = ArrayNetworkService.this.connections
        // Loop waiting for inbound connections
        while (true) {
          // Accept the client socket
          val clientChannel: SocketChannel = serverChannel.accept
          clientChannel.socket.setTcpNoDelay(tcpNoDelay)
          //println("Receiving Connection")
          // Get the bytes encoding the source partition Id
          var connectionId: NetworkDestinationHandle = -1;
          if(performIDHandshake) {
            val bytes = new Array[Byte](4)
            val bytesRead = clientChannel.socket.getInputStream.read(bytes)
            assert(bytesRead == 4)
            // Read the partition id
            connectionId = new DataInputStream(new ByteArrayInputStream(bytes)).readInt()
          } else {
            connectionId = nextConnectionID.decrementAndGet();
          }
          // register the connection, which will start a reader
          _registerConnection(connectionId, clientChannel)
        }
      }
    }.start()
  }

  override def send(dst: NetworkDestinationHandle, buffer: Array[Byte]) {
    val sockBuf = connections.get(dst)

    // TODO: Something if buffer is null

    while (!sockBuf.write(buffer)) {
      // If we can't write, try to send
      if (sockBuf.sending.compareAndSet(false,true)) {
        executor.submit(sockBuf)
      }
      Thread.sleep(5)
    }
  }

  override def sendAny(buffer: Array[Byte]) {
    if(connections.isEmpty) {
      logger.error("Empty connections list in sendAny!")
    }

    val connArray = connections.keySet.toArray
    send(connArray(Random.nextInt(connArray.length)).asInstanceOf[NetworkDestinationHandle], buffer)
  }

  override def disconnect(which: NetworkDestinationHandle) {
    if(connections.contains(which)) {
      logger.error(s"Disconnection to $which requested, but $which not found!")
      return
    }

    connections.remove(which)
  }

}

