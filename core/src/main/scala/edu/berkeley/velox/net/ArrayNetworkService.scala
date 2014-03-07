package edu.berkeley.velox.net

import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.NetworkDestinationHandle
import edu.berkeley.velox.conf.VeloxConfig
import edu.berkeley.velox.rpc.MessageService
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.net.{SocketOptions, InetSocketAddress}
import java.nio.ByteBuffer
import java.nio.channels.{ServerSocketChannel, SocketChannel}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentHashMap, ExecutorService, Executors, LinkedBlockingQueue, Semaphore, ThreadFactory}
import java.util.concurrent.atomic.{AtomicBoolean,AtomicInteger}
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.collection.mutable.StringBuilder
import scala.util.Random
import scala.collection.JavaConverters._

class SocketBuffer(
  channel: SocketChannel,
  pool: SocketBufferPool) extends Logging {

  var buf = ByteBuffer.allocateDirect(VeloxConfig.bufferSize)
  buf.position(4)

  val writePos = new AtomicInteger(4)

  val lastPos = new AtomicInteger(0)

  val rwlock = new ReentrantReadWriteLock()


  /** Write bytes into this buffer
    *
    * @param bytes The bytes to write
    * @param isSwapper Only to be set to true by the swap method
    *                  to bypass the currentBuffer check
    *
    * @return true if data was written successfully, false otherwise
    */
  def write(bytes: ByteBuffer, isSwapper: Boolean = false): Boolean = {
    if (!rwlock.readLock.tryLock) {
      if (isSwapper)
        logger.warn("returning false on the swapper because can't get read lock (this is probably okay)")
      return false
    }

    // ensure we're still looking at the right buffer
    if (!isSwapper && pool.currentBuffer != this) {
      rwlock.readLock.unlock
      return false
    }

    val len = bytes.remaining

    val writeOffset = writePos.getAndAdd(len)
    val endPos = writeOffset+len
    val ret =
      if (endPos <= buf.limit) {
        val dup = buf.duplicate
        dup.position(writeOffset)
        dup.put(bytes)

        // We loop here and compareAndSet to make sure
        // that the value we compare against hasn't been
        // changed.  This prevents setting lastPos back
        // to something smaller than it should be
        var lpSet = false
        while (!lpSet) {
          val cur = lastPos.get
          if (endPos > cur)
            lpSet = lastPos.compareAndSet(cur,endPos)
          else
            lpSet = true
        }

        rwlock.readLock.unlock
        true
      }
      else {
        // can't upgrade to write lock, so unlock
        rwlock.readLock.unlock
        rwlock.writeLock.lock
        // recheck in case someone else got it
        if (pool.currentBuffer == this) {
          val r = pool.swap(bytes)
          send
          rwlock.writeLock.unlock
          pool.returnBuffer(this)
          r
        }
        else {
          if (isSwapper)
            logger.error("Swapper trying to write more bytes than buffer size")
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
  def send() {
    try {
      if (lastPos.get > 4) {
        buf.position(0)

        // write out full message length (minus 4 for these bytes)
        buf.putInt(lastPos.get-4)
        buf.limit(lastPos.get)
        buf.position(0)

        // wrap the array and write it out
        val wrote = channel.write(buf)
        assert(wrote == lastPos.get)
        pool.lastSent = System.currentTimeMillis

        // reset write position
        buf.clear
        buf.position(4)
        writePos.set(4)
        lastPos.set(0)
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

class SocketBufferPool(channel: SocketChannel)  {

  val pool = new LinkedBlockingQueue[SocketBuffer]()
  @volatile var currentBuffer: SocketBuffer = new SocketBuffer(channel,this)
  @volatile var lastSent = 0l

  val isForceSending = new AtomicBoolean(false)

  // Create an runnable that calls forceSend so we
  // don't have to create a new object every time
  val forceRunner = new Runnable() {
    def run() = forceSend()
  }

  def needSend(): Boolean = {
    if ( (currentBuffer.writePos.get > 4) &&
         ((System.currentTimeMillis - lastSent) > VeloxConfig.sweepTime) ) {
      isForceSending.compareAndSet(false,true)
    } else false
  }

  def send(bytes: ByteBuffer) {
    while(!currentBuffer.write(bytes)) {}
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
        newBuf.write(bytes,true)
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
    buf.rwlock.writeLock.lock()
    var didsend = false
    if (currentBuffer == buf && buf.writePos.get > 4) {
      swap(null)
      buf.send
      didsend = true
    }
    buf.rwlock.writeLock.unlock()
    if (didsend)
      returnBuffer(buf)
    isForceSending.set(false)
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

    if(bytes.capacity() > ProcessingBufferPool.minCapacity) {
      bytes.clear()
      ProcessingBufferPool.pool.add(bytes)
    }
  } catch {
    case t: Throwable => {
      logger.error(s"Error receiving message: ${t.getMessage}",t)
    }
  }

}

object ProcessingBufferPool {
  val pool = new LinkedBlockingQueue[ByteBuffer]()
  val minCapacity = VeloxConfig.bufferSize/2
}

class ReaderThread(
  name: String,
  channel: SocketChannel,
  executor: ExecutorService,
  src: NetworkDestinationHandle,
  messageService: MessageService,
  remoteAddr: String) extends Thread(s"Reader-${name} from ${remoteAddr}") with Logging {

  val readInput = channel.socket().getInputStream

  override def run() {
    val readBuffer = ByteBuffer.allocateDirect(VeloxConfig.bufferSize)
    while(true) {
      var read = readBuffer.position
      read += channel.read(readBuffer)

      var cleared = false

      if (read >= 4) {
        readBuffer.flip

        var len = readBuffer.getInt

        if (readBuffer.remaining == len) { // perfect read
          var msgBuf = ProcessingBufferPool.pool.poll()// ByteBuffer.allocate(len)

          if(msgBuf == null || msgBuf.capacity() < len) {
            msgBuf = ByteBuffer.allocate(VeloxConfig.bufferSize)
          } else {
            msgBuf.clear()
          }
          msgBuf.put(readBuffer)
          msgBuf.flip()
          executor.submit(new Receiver(msgBuf,src,messageService))

          cleared = true
          readBuffer.clear()

          len = -1 // prevent attempt to copy len below
        }
        else {

          while (readBuffer.remaining >= 4 && readBuffer.remaining >= len) { // read enough
            if (len > VeloxConfig.bufferSize) {
              println(s"OHH NO LEN TO BIG $len")
            }
            var msgBuf = ProcessingBufferPool.pool.poll()// ByteBuffer.allocate(len)

            if(msgBuf == null || msgBuf.capacity() < len) {
              msgBuf = ByteBuffer.allocate(VeloxConfig.bufferSize)
            } else {
              msgBuf.clear()
            }

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

        if(!cleared) {
          readBuffer.compact()
        }
      }
    }
  }
}

class SendSweeper(
  connections: ConcurrentHashMap[NetworkDestinationHandle, SocketBufferPool],
  executor: ExecutorService) extends Runnable with Logging {

  def run() {
    while(true) {
      if(VeloxConfig.sweepTime > 0)
        Thread.sleep(VeloxConfig.sweepTime)

      try {
        val cit = connections.keySet.iterator
        while (cit.hasNext) {
          val sp = connections.get(cit.next)
          if (sp.needSend)
            executor.submit(sp.forceRunner)
        }
      }
      catch { // TODO: Should we stop the sweeper, or pause?
        case t: Throwable => logger.error("Error send sweeping",t)
      }

    }
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
        Executors.newFixedThreadPool(VeloxConfig.pool_threads,new ArrayNetworkThreadFactory("ANS"))
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
    new Thread(new SendSweeper(connections,executor), s"Sweeper-").start
  }

  override def connect(handle: NetworkDestinationHandle, address: InetSocketAddress) {
    while(true) {
      try {
        val clientChannel = SocketChannel.open()
        clientChannel.connect(address)
        clientChannel.socket.setTcpNoDelay(tcpNoDelay)

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
      new ReaderThread(name, channel, executor, partitionId, messageService, channel.getRemoteAddress.toString).start
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
