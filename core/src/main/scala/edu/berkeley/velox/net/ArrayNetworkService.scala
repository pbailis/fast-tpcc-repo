package edu.berkeley.velox.net

import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.NetworkDestinationHandle
import edu.berkeley.velox.rpc.MessageService
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{ServerSocketChannel, SocketChannel}
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors, Semaphore, ThreadFactory}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.collection.mutable.StringBuilder
import scala.util.Random

object HackConfig {
  val bufSize = 16384
}

class SocketBuffer(channel: SocketChannel) extends Runnable {
  var buf = ByteBuffer.allocate(HackConfig.bufSize)
  buf.position(4)

  val writePos = new AtomicInteger(4)

  val writers = new AtomicInteger(0)
  val sending = new AtomicBoolean(false)


  /** Write bytes into this buffer
    *
    * @param bytes The bytes to write
    */
  def write(bytes: ByteBuffer): Boolean = {
    // indicate our intent to write
    writers.incrementAndGet

    // fail if we're already sending this buffer
    if (sending.get) {
      writers.decrementAndGet
      return false
    }

    val len = bytes.remaining

    val writeOffset = writePos.getAndAdd(len)
    val ret =
      if (writeOffset + len <= buf.limit) {
        val dup = buf.duplicate
        dup.position(writeOffset)
        dup.put(bytes)
        true
      }
      else {
        writePos.getAndAdd(-len)
        false
      }

    writers.decrementAndGet
    ret
  }

  /**
    * Calling this class as a runnable sends the current data
    *
    * sending MUST be set to true before asking this class to run
    */
  def run() {
    try {
      // wait for any writers to finish
      while (writers.get > 0) {
        Thread.sleep(1)
      }

      if (writePos.get > 4) {
        buf.position(0)

        // write out full message length (minus 4 for these bytes)
        buf.putInt(writePos.get-4)
        buf.limit(writePos.get)
        buf.position(0)

        // wrap the array and write it out
        val wrote = channel.write(buf)

        // reset write position
        buf.clear
        buf.position(4)
        writePos.set(4)
      }

      sending.set(false)

      this.synchronized { this.notifyAll }

    } catch {
      case e: Exception => {
        println("EXCEPTION HERE: "+e.getMessage)
        sending.set(false)
        throw e
      }
    }
  }

  def id(): Int = System.identityHashCode(this)

  def printStatus() {
    val builder = new StringBuilder(s"${id} [${channel.getLocalAddress.toString} <-> ${channel.getRemoteAddress.toString}] - ")
    builder append s" pos: ${writePos.get}"
    builder append s" sending: ${sending.get}"
    builder append s" writers: ${writers.get}"
    println(builder.result)
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
    while(bytes.remaining != 0) {
      messageService.receiveRemoteMessage(src,bytes)
    }
  }

}

class ReaderThread(
  channel: SocketChannel,
  executor: ExecutorService,
  src: NetworkDestinationHandle,
  messageService: MessageService,
  remoteAddr: String) extends Thread(s"Reader from ${remoteAddr}") {

  override def run() {
    var readBuffer = ByteBuffer.allocate(HackConfig.bufSize)
    while(true) {
      var read = readBuffer.position
      read += channel.read(readBuffer)

      var allocedBuffer = false

      if (read >= 4) {
        readBuffer.flip

        var len = readBuffer.getInt

        if (readBuffer.remaining == len) { // perfect read
          executor.submit(new Receiver(readBuffer,src,messageService))
          readBuffer = ByteBuffer.allocate(HackConfig.bufSize)
          allocedBuffer = true
          len = -1 // prevent attempt to copy len below
        }
        else {

          while (readBuffer.remaining >= 4 && readBuffer.remaining >= len) { // read enough
            if (len > HackConfig.bufSize) {
              println(s"OHH NO LEN TO BIG $len")
            }
            val msgBuf = ByteBuffer.allocate(len)
            val oldLim = readBuffer.limit
            readBuffer.limit(readBuffer.position+len)
            msgBuf.put(readBuffer)
            readBuffer.limit(oldLim)
            msgBuf.flip
            executor.submit(new Receiver(msgBuf,src,messageService))
            if (readBuffer.remaining >= 4)
              len = readBuffer.getInt
            else
              len = -1 // indicate we can't put the whole int
          }
        }

        if (len != -1) {
          readBuffer.position(readBuffer.position-4)
          readBuffer.putInt(len)
          readBuffer.position(readBuffer.position-4)
        }

        if (!allocedBuffer) // compact on a new buffer is bad
          readBuffer.compact
      }
    }
  }
}

class SendSweeper(
  connections: ConcurrentHashMap[NetworkDestinationHandle, SocketBuffer],
  executor: ExecutorService) extends Runnable {

  var i = 0

  def run() {
    while(true) {
      val cit = connections.keySet.iterator
      while (cit.hasNext) {
        val buf = connections.get(cit.next)
        if (buf.writePos.get > 4 && buf.sending.compareAndSet(false,true)) {
          //executor.submit(buf)
          buf.run()
        }
      }
      Thread.sleep(500)
      i+=1
    }
  }
}

class ArrayNetworkThreadFactory extends ThreadFactory {

  val defaultFactory = Executors.defaultThreadFactory
  var threadIdx = 0

  override
  def newThread(r: Runnable):Thread = {
    val t = defaultFactory.newThread(r)
    t.setName(s"ArrayNetworkServiceThread-$threadIdx")
    threadIdx+=1
    t
  }
}

class ArrayNetworkService(
  val performIDHandshake: Boolean = false,
  val tcpNoDelay: Boolean = true,
  val serverID: Integer = -1) extends NetworkService with Logging {

  val executor = Executors.newFixedThreadPool(16,new ArrayNetworkThreadFactory())
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
      new ReaderThread(channel,executor,partitionId,messageService,channel.getRemoteAddress.toString).start
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

  override def send(dst: NetworkDestinationHandle, buffer: ByteBuffer) {
    val sockBuf = connections.get(dst)

    // TODO: Something if buffer is null

    while (!sockBuf.write(buffer)) {
      // If we can't write, try to send
      if (sockBuf.sending.compareAndSet(false,true)) {
        //executor.submit(sockBuf)
        sockBuf.run
      } else if (sockBuf.sending.get) {
        sockBuf.synchronized {
          // recheck in case I just finished
          if (sockBuf.sending.get)
            sockBuf.wait
        }
      } else // something odd happening, wait and try again shortly
        Thread.sleep(10)
    }
  }

  override def sendAny(buffer: ByteBuffer) {
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

