package edu.berkeley.velox.net

import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.NetworkDestinationHandle
import edu.berkeley.velox.conf.VeloxConfig
import edu.berkeley.velox.rpc.MessageService
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{ServerSocketChannel, SocketChannel}
import java.util.concurrent.atomic.{AtomicLong, AtomicBoolean, AtomicInteger}
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors, LinkedBlockingQueue, Semaphore, ThreadFactory}
import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}
import scala.collection.mutable.StringBuilder
import scala.util.Random
import scala.collection.JavaConverters._
import edu.berkeley.velox.util.KryoThreadLocal


object SendStats {
  val numSent = new AtomicLong
  val bytesSent = new AtomicLong
  val numRecv = new AtomicLong
  val bytesRecv = new AtomicLong
  val tryRecv = new AtomicLong
  val tryBytesRecv = new AtomicLong

}

class SocketBuffer(
  channel: SocketChannel,
  pool: SocketBufferPool) extends Logging {

  /** Write bytes into this buffer
    *
    * @param bytes The bytes to write
    *
    * @return true if data was written successfully, false otherwise
    */
  def write(bytes: ByteBuffer): Boolean = {
    val len = (bytes.remaining())
      val intBuf = ByteBuffer.allocate(4+len)
      intBuf.putInt(len)
      intBuf.put(bytes)
      intBuf.flip()
      //channel.socket().getOutputStream.write(intBuf.array())
      //val byteArr = new Array[Byte](len)
      //bytes.get(byteArr)
      //channel.socket().getOutputStream.write(byteArr)
      //channel.socket().getOutputStream.flush()
      val wrote = channel.write(bytes)

      assert(wrote == len+4)

      SendStats.bytesSent.addAndGet(len+4)
      SendStats.numSent.incrementAndGet()

      return true
  }

  def id(): Int = System.identityHashCode(this)

  def printStatus() {
    val builder = new StringBuilder(s"${id} [${channel.socket().getLocalAddress.toString} <-> ${channel.socket().getRemoteSocketAddress.toString}] - ")
    println(builder.result)
  }

}

class SocketBufferPool(channel: SocketChannel)  {
  @volatile var currentBuffer: SocketBuffer = new SocketBuffer(channel,this)


  def needSend(): Boolean = {
    false
  }

  def send(bytes: ByteBuffer) {
    var sent = false
    while(!sent) {
      sent = currentBuffer.write(bytes)
    }
  }

}

class Receiver(
  bytes: ByteBuffer,
  src: NetworkDestinationHandle,
  messageService: MessageService) extends Runnable with Logging {

  def run() = try {
    while(bytes.remaining != 0) {
      try {
        messageService.receiveRemoteMessage(src,bytes)
      } catch {
        case e: Exception => logger.error("Error receiving message", e)
      }
    }
  } catch {
    case t: Throwable => {
      logger.error(s"Error receiving message: ${t.getMessage}",t)
    }
  }

}

class ReaderThread(
  name: String,
  channel: SocketChannel,
  executor: ExecutorService,
  src: NetworkDestinationHandle,
  messageService: MessageService,
  remoteAddr: String) extends Thread(s"Reader-${name} from ${remoteAddr}") with Logging {

  override def run() {
    var readBuffer = ByteBuffer.allocate(VeloxConfig.bufferSize)
    var missing = -1
    while(true) {

      val intBuf = new Array[Byte](4)
      channel.socket().getInputStream.read(intBuf)
      val len = ByteBuffer.wrap(intBuf).getInt()

      SendStats.tryRecv.incrementAndGet()
      SendStats.tryBytesRecv.addAndGet(len)


      var readBytes = 0
      val msgArr = new Array[Byte](len)
       readBytes += channel.socket().getInputStream.read(msgArr)
      assert(readBytes == len)

      SendStats.bytesRecv.addAndGet(len+4)
      SendStats.numRecv.incrementAndGet()

      val msgBuf = ByteBuffer.wrap(msgArr)

      executor.submit(new Receiver(msgBuf, src, messageService))

      /*

      var intBuf = ByteBuffer.allocate(4)
      channel.read(intBuf)
      intBuf.flip()
      val len = intBuf.getInt()

      assert(len != 0)

      SendStats.tryRecv.incrementAndGet()
      SendStats.tryBytesRecv.addAndGet(len)

      var readBytes = 0

      val msgBuf = ByteBuffer.allocate(len)
      while(readBytes != len) {
        val read = channel.read(msgBuf)
        if(read == -1) {
          logger.error(s"read negative one!")
        }
        readBytes += read
      }

      msgBuf.flip()

      SendStats.bytesRecv.addAndGet(len+4)
      SendStats.numRecv.incrementAndGet()

      executor.submit(new Receiver(msgBuf, src, messageService))
      */

      /*

      if(missing != -1) {
        logger.error(s"missing $missing bytes! $readBuffer")
      }

      var read = readBuffer.position

      read += channel.read(readBuffer)

      if(missing != -1) {
        logger.error(s"was missing $missing bytes! $read $readBuffer")
      }


      var allocedBuffer = false

      if (read >= 4) {
        readBuffer.flip

        var len = readBuffer.getInt

        if (readBuffer.remaining == len) { // perfect read
          executor.submit(new Receiver(readBuffer,src,messageService))

          readBuffer = ByteBuffer.allocate(VeloxConfig.bufferSize)
          allocedBuffer = true
          len = -1 // prevent attempt to copy len below
        }
        else {

          while (len != -1 && readBuffer.remaining >= len) { // read enough
            if (len > VeloxConfig.bufferSize) {
              println(s"OHH NO LEN TO BIG $len")
            }

            val msgBuf = ByteBuffer.allocate(len)
            val oldLim = readBuffer.limit
            readBuffer.limit(readBuffer.position+len)
            msgBuf.put(readBuffer)
            readBuffer.limit(oldLim)
            msgBuf.flip
            executor.submit(new Receiver(msgBuf,src,messageService))

            if (readBuffer.remaining > 4)
              len = readBuffer.getInt
            else
              len = -1 // indicate we can't put the whole int
          }
        }

        if (len != -1) {
          missing = len

          readBuffer.position(readBuffer.position-4)
          readBuffer.putInt(len)
          readBuffer.position(readBuffer.position-4)
        } else {
          missing = -1
        }

        if (!allocedBuffer) // compact on a new buffer is bad
          readBuffer.compact
      }
      */
    }
  }
}

class SendSweeper(
  connections: ConcurrentHashMap[NetworkDestinationHandle, SocketBufferPool],
  executor: ExecutorService) extends Runnable with Logging {

  def run() {

  }

}

class ArrayNetworkThreadFactory(val name: String) extends ThreadFactory {

  val defaultFactory = Executors.defaultThreadFactory
  var threadIdx = new AtomicInteger(0)

  override
  def newThread(r: Runnable):Thread = {
    val t = defaultFactory.newThread(r)
    val tid = threadIdx.getAndIncrement()
    t.setName(s"ArrayNetworkServiceThread-$name-$tid")
    t
  }
}

class ArrayNetworkService(val performIDHandshake: Boolean = false,
  val tcpNoDelay: Boolean = true,
  val serverID: Integer = -1) extends NetworkService with Logging {

  val name = "ANS"

  var executor: ExecutorService = null

  def setExecutor(toSet: ExecutorService = null): ExecutorService = {
    if(toSet != null) {
      this.executor = toSet
      return this.executor
    }

    if(!VeloxConfig.serializable) {
      this.executor =
        Executors.newCachedThreadPool()//Executors.newFixedThreadPool(32,new ArrayNetworkThreadFactory("ANS"))
      } else {
      this.executor =
        Executors.newCachedThreadPool()
    }

    this.executor
  }

  new Thread(new Runnable {
    override def run() {
      while (true) {
        logger.error(s"S ${SendStats.numSent} ${SendStats.bytesSent} R ${SendStats.numRecv} ${SendStats.bytesRecv} T ${SendStats.tryRecv} ${SendStats.tryBytesRecv}")
        Thread.sleep(1000)
      }
    }
  }).start()


  val connections = new ConcurrentHashMap[NetworkDestinationHandle, SocketBufferPool]
  val nextConnectionID = new AtomicInteger(0)
  private val connectionSemaphore = new Semaphore(0)

  override def setMessageService(messageService: MessageService) {
    this.messageService = messageService
  }

  def blockForConnections(numConnections: Integer) {
    connectionSemaphore.acquireUninterruptibly(numConnections)
  }

  def start() {
    new Thread(new SendSweeper(connections,executor), s"Sweeper-").start
  }

  override def connect(handle: NetworkDestinationHandle, address: InetSocketAddress) {
    while(true) {
      try {
        val clientChannel = SocketChannel.open()
        clientChannel.connect(address)
        clientChannel.socket().setTcpNoDelay(true)
        assert(clientChannel.isConnected)


        if(performIDHandshake) {
          val shakeArray = ByteBuffer.allocate(4)
          shakeArray.putInt(serverID)
          shakeArray.flip()
          clientChannel.write(shakeArray)
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
    val bufPool = new SocketBufferPool(channel)
    if (connections.putIfAbsent(partitionId,bufPool) == null) {
      logger.info(s"Adding connection from $partitionId")
      // start up a read thread
      new ReaderThread(name, channel, executor, partitionId, messageService, channel.socket().getRemoteSocketAddress.toString).start
      connectionSemaphore.release
    }
  }

  override def configureInboundListener(port: Integer) {
    val serverChannel = ServerSocketChannel.open()
    logger.info("Listening on: "+port)
    serverChannel.socket.bind(new InetSocketAddress(port))

    val connectionListener = new Thread {
      override def run() {
        // Grab references to memebers in the parent class
        val connections = ArrayNetworkService.this.connections
        // Loop waiting for inbound connections
        while (true) {
          // Accept the client socket
          val clientChannel: SocketChannel = serverChannel.accept
          clientChannel.socket.setTcpNoDelay(true)
          // Get the bytes encoding the source partition Id
          var connectionId: NetworkDestinationHandle = -1;
          if(performIDHandshake) {
            val shakeBuf = ByteBuffer.allocate(4)
            while(shakeBuf.hasRemaining) {
              val bytesRead = clientChannel.read(shakeBuf)
            }
            // Read the partition id
            shakeBuf.rewind()
            connectionId = shakeBuf.getInt()
          } else {
            connectionId = nextConnectionID.decrementAndGet();
          }
          // register the connection, which will start a reader
          _registerConnection(connectionId, clientChannel)
        }
      }
    }
    connectionListener.setName(s"ConnectionListener-${name}")
    connectionListener.start()
  }

  override def send(dst: NetworkDestinationHandle, buffer: ByteBuffer) {
    val sockBufPool = connections.get(dst)
    // TODO: Something if buffer is null
    sockBufPool.send(buffer)
  }

  override def sendAny(buffer: ByteBuffer) {
    if(connections.isEmpty) {
      logger.error("Empty connections list in sendAny!")
    }

    val connArray = connections.keySet.toArray
    val recvr = Random.nextInt(connArray.length)
    send(connArray(recvr).asInstanceOf[NetworkDestinationHandle], buffer)
  }

  override def disconnect(which: NetworkDestinationHandle) {
    if(connections.contains(which)) {
      logger.error(s"Disconnection to $which requested, but $which not found!")
      return
    }

    connections.remove(which)
  }

}
