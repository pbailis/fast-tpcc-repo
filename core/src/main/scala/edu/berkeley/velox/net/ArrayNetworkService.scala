package edu.berkeley.velox.net

import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.NetworkDestinationHandle
import edu.berkeley.velox.conf.VeloxConfig
import edu.berkeley.velox.rpc.MessageService
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{ServerSocketChannel, SocketChannel}
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors, LinkedBlockingQueue, Semaphore, ThreadFactory}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.{ReentrantLock, ReentrantReadWriteLock}
import scala.collection.mutable.StringBuilder
import scala.util.Random

class SocketBuffer(
  channel: SocketChannel,
  pool: SocketBufferPool) extends Logging {

  var buf = ByteBuffer.allocate(VeloxConfig.bufferSize)
  buf.position(4)

  val writePos = new AtomicInteger(4)

  val rwlock = new ReentrantReadWriteLock()


  /** Write bytes into this buffer
    *
    * @param bytes The bytes to write
    *
    * @return true if data was written successfully, false otherwise
    */
  def write(bytes: ByteBuffer): Boolean = {
    if (!rwlock.readLock.tryLock)
      return false

    // ensure we're still looking at the right buffer
    if (pool.currentBuffer != this) {
      rwlock.readLock.unlock
      return false
    }

    logger.error(s"writing $bytes to $this $buf ${writePos.get()}")


    val len = bytes.remaining

    val writeOffset = writePos.getAndAdd(len)
    val ret =
      if (writeOffset + len <= buf.limit) {
        val dup = buf.duplicate
        dup.position(writeOffset)
        dup.put(bytes)
        rwlock.readLock.unlock
        true
      }
      else {
        writePos.getAndAdd(-len)
        // can't upgrade to write lock, so unlock
        rwlock.readLock.unlock
        rwlock.writeLock.lock
        // recheck in case someone else got it
        if (pool.currentBuffer == this && writePos.get > 4) {
          val r = pool.swap(bytes)
          send(false)
          rwlock.writeLock.unlock
          pool.returnBuffer(this)
          r
        }
        else {
          rwlock.writeLock.unlock
          false
        }
      }

    ret
  }

  /**
    * Send the current data
    *
    * The write lock MUST be held before calling this method
    *
    */
  def send(forced: Boolean) {
    try {
      if (writePos.get > 4) {
        buf.position(0)

        // write out full message length (minus 4 for these bytes)
        buf.putInt(writePos.get-4)
        buf.limit(writePos.get)
        buf.position(0)

        // wrap the array and write it out
        while(buf.hasRemaining) {
          channel.write(buf)
        }
        pool.lastSent = System.currentTimeMillis

        // reset write position
        buf.clear
        buf.position(4)
        writePos.set(4)
      }
    } catch {
      case e: Exception => {
        println("EXCEPTION HERE: "+e.getMessage)
        e.printStackTrace
        throw e
      }
    }
  }

  def id(): Int = System.identityHashCode(this)

  def printStatus() {
    val builder = new StringBuilder(s"${id} [${channel.getLocalAddress.toString} <-> ${channel.getRemoteAddress.toString}] - ")
    builder append s" pos: ${writePos.get}"
    println(builder.result)
  }

}

class SocketBufferPool(channel: SocketChannel) extends Logging {
  val pool = new LinkedBlockingQueue[SocketBuffer]()
  @volatile var currentBuffer: SocketBuffer = new SocketBuffer(channel,this)
  @volatile var lastSent = 0l
  @volatile var sweeping = false

  // Create an runnable that calls forceSend so we
  // don't have to create a new object every time
  val forceRunner = new Runnable() {
    def run() = forceSend()
  }

  def needSend(): Boolean = {
    if(sweeping == false && currentBuffer.writePos.get > 4 && (System.currentTimeMillis - lastSent) > VeloxConfig.sweepTime) {
      sweeping = true
      return true
    }

    return false
  }

  def send(bytes: ByteBuffer) {
    var sent = false
    while(!sent) {
      sent = currentBuffer.write(bytes)
    }
  }

  /** Swap the active buffer.  This should only be called by a thread
    * holding a write lock on currentBuffer
    *
    * @param bytes Data to write into the buffer that will be active after
    *              this call, or null to write nothing
    *
    * @return true if requested bytes written successfully into new buffer
    */

  def swap(bytes: ByteBuffer):Boolean = {
    var newBuf = pool.poll

    // TODO: Should probably have a limit on the number of buffers we create
    if (newBuf == null)
      newBuf = new SocketBuffer(channel,this)

    val ret =
      if (bytes != null)
        newBuf.write(bytes)
      else
        false

    currentBuffer = newBuf

    ret

  }

  def returnBuffer(buf: SocketBuffer) = pool.put(buf)

  /**
    * Force the current buffer to be sent immediately
    */
  def forceSend() {
    val buf = currentBuffer
    logger.error(s"forcesend on $buf ${buf.buf} ${buf.writePos}")

    var writeBytes = -1

    buf.rwlock.writeLock.lock()
    var didsend = false
    if (currentBuffer == buf && buf.writePos.get > 4) {

      logger.error(s"forcesending 1 on $buf ${buf.buf} ${buf.writePos}")

      writeBytes = buf.writePos.get()


      swap(null)

      logger.error(s"forcesending 2 on $buf ${buf.buf} ${buf.writePos}")

      buf.send(true)

      logger.error(s"forcesending 3 on $buf ${buf.buf} ${buf.writePos}")


      didsend = true
    }
    buf.rwlock.writeLock.unlock()
    if (didsend)
      returnBuffer(buf)

    logger.error(s"finished forcesend on $buf ${buf.buf} $didsend $writeBytes")

    sweeping = false
  }
}

class Receiver (
  bytes: ByteBuffer,
  src: NetworkDestinationHandle,
  messageService: MessageService) extends Runnable with Logging {

  def run() = try {
      while(bytes.remaining != 0) {
        messageService.receiveRemoteMessage(src,bytes)
      }
   } catch {
     case t: Throwable => {
       logger.error(s"Error receiving message: ${t.getMessage}",t)
     }
  }
}

class ReaderThread (
  channel: SocketChannel,
  executor: ExecutorService,
  src: NetworkDestinationHandle,
  messageService: MessageService,
  remoteAddr: String) extends Thread(s"Reader from ${remoteAddr}") with Logging {

  override def run() {
    var readBuffer = ByteBuffer.allocate(VeloxConfig.bufferSize)
    while(true) {
      var read = readBuffer.position
      read += channel.read(readBuffer)

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

          while (readBuffer.remaining >= 4 && readBuffer.remaining >= len) { // read enough
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
  connections: ConcurrentHashMap[NetworkDestinationHandle, SocketBufferPool],
  executor: ExecutorService) extends Runnable {

  def run() {
    while(true) {
      if(VeloxConfig.sweepTime > 0)
        Thread.sleep(VeloxConfig.sweepTime)
      val cit = connections.keySet.iterator
      while (cit.hasNext) {
        val sp = connections.get(cit.next)
        if (sp.needSend)
          executor.submit(sp.forceRunner)
      }
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

  var executor: ExecutorService = null

  def setExecutor(toSet: ExecutorService = null): ExecutorService = {
    if(toSet != null) {
      this.executor = toSet
      return this.executor
    }

    if(!VeloxConfig.serializable) {
      this.executor =
        Executors.newFixedThreadPool(32,new ArrayNetworkThreadFactory())
      } else {
      this.executor =
        Executors.newCachedThreadPool()
    }

    this.executor
  }

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
    val bufPool = new SocketBufferPool(channel)
    if (connections.putIfAbsent(partitionId,bufPool) == null) {
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
    val sockBufPool = connections.get(dst)
    // TODO: Something if buffer is null
    sockBufPool.send(buffer)
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
