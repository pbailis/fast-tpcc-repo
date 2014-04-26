package edu.berkeley.velox.net

import java.net.InetSocketAddress
import java.nio.channels.{SelectionKey, SocketChannel, Selector, ServerSocketChannel}
import java.util.concurrent._
import edu.berkeley.velox.NetworkDestinationHandle
import java.io.{DataOutputStream, ByteArrayOutputStream, ByteArrayInputStream, DataInputStream}
import java.nio.ByteBuffer
import com.typesafe.scalalogging.slf4j.Logging
import scala.util.Random
import java.util.concurrent.atomic.AtomicInteger
import edu.berkeley.velox.rpc.MessageService
import scala.collection.JavaConversions._
import edu.berkeley.velox.util.VeloxFixedThreadPool

class NIONetworkService(val name: String,
                        val performIDHandshake: Boolean = false,
                        val tcpNoDelay: Boolean = true,
                        val serverID: Integer = -1) extends NetworkService with Logging {

  class ChannelState (val partitionId: NetworkDestinationHandle,
                      val channel: SocketChannel,
                      val mb: Int = 1) {
    var isOpen = true;
    val writeBuffers = new RingBuffer(mb)
    var readBuffer: ByteBuffer = ByteBuffer.allocateDirect(mb * 1048576)

    val nextInternalID = new AtomicInteger(0)

    def writeMessage(bytes: Array[Byte]): Boolean = {
      if(!isOpen) {
        logger.error(s"Write to closed channel ($channel)")
      }

      val bufferResized = writeBuffers.writeMessage(bytes)
      messageSentMeter.mark()
      bytesWrittenMeter.mark(bytes.size)
      bufferResized
    }

    def shutdown {
      isOpen = false
      channel.close()
    }
  }

  /**
   * A single writer thread multiplexes writing on each socket
   */
  class WriterThread extends Thread("Write-Selector-Thread") {
    val selector = Selector.open
    val newChannels = collection.mutable.Queue.empty[ChannelState]
    override def run() {
      while(true) {
        val count = selector.select()
        // Register any pending write selectors
        newChannels.synchronized {
          if(!newChannels.isEmpty) {
            for(cs <- newChannels) {
              cs.channel.register(selector, SelectionKey.OP_WRITE, cs)
            }
            newChannels.clear()
          }
        }
        val it = selector.selectedKeys().iterator()
        var bytesWrittenOnRound = 0L
        while (it.hasNext) {
          val key = it.next()
          it.remove()
          // if the channel is ready to send bytes
          if (key.isValid && key.isWritable) {
            // Get the channel and socket thread
            val channel = key.channel.asInstanceOf[SocketChannel]
            val state = key.attachment().asInstanceOf[ChannelState]
            val buffers = state.writeBuffers
            // Test if there are any bytes remaining in the send buffer
            var hasRemaining = buffers.pending.exists(_.hasRemaining)
            // If the pending send buffer is empty try and get more to send
            if (!hasRemaining) {
              logger.trace("flipping send buffer")
              buffers.finishedSending()
              hasRemaining = buffers.pending.exists(_.hasRemaining)
            }
            // If there are bytes to send then try and send those bytes
            if (hasRemaining) {
              // write the bytes from the buffer
              var bytesWritten = channel.write(buffers.pending)
              var totalBytesWritten = bytesWritten

              while (bytesWritten > 0 && buffers.pending.exists(_.hasRemaining)) {
                bytesWritten = channel.write(buffers.pending)
                assert(bytesWritten >= 0)
                totalBytesWritten += bytesWritten
              }

              bytesWrittenOnRound += totalBytesWritten
              //println(s"Total bytes written: $totalBytesWritten")
              bytesSentMeter.mark(totalBytesWritten)
            }
          }
        } // end of while loop over iterator
        // if(bytesWrittenOnRound == 0) {
        //   synchronized { wait(100) }
        // }
      } // end of outer while loop
    } // end of run
  } // end of writer thread

  def processFrames(partition: NetworkDestinationHandle, buffer: ByteBuffer) {
    // Process the read buffer (we make a local copy here)
    // readExecutor.execute(new Runnable {
    //   def run() {
    //     while (buffer.hasRemaining) {
    //       val frameSize = buffer.getInt
    //       assert(frameSize <= buffer.remaining)
    //       val endOfFrame = buffer.position + frameSize
    //       while (buffer.position < endOfFrame) {
    //         val msgSize = buffer.getInt
    //         assert(msgSize >= 0)
    //         val bytes = new Array[Byte](msgSize)
    //         buffer.get(bytes)
    //         NIONetworkService.this.receive(partition, bytes)
    //       }
    //     }
    //     // We should have now depleted the buffer
    //     assert(!buffer.hasRemaining)
    //     buffer.clear()
    //     readBufferPool.put(buffer)
    //   }
    // })
  }

  def getNewReadBuffer(minSize: Int): ByteBuffer = {
    // Get a buffer
    var nextBuffer = readBufferPool.take()
    if (nextBuffer.capacity < minSize) {
      var newSize = nextBuffer.capacity()
      while (newSize < minSize) { newSize = 2 * newSize }
      // return the old buffer (maybe we can reuse it?)
      readBufferPool.put(nextBuffer)
      nextBuffer = ByteBuffer.allocateDirect(newSize)
    }
    nextBuffer
  }

  class ReaderThread extends Thread("Read-Selector-Thread") {
    val selector = Selector.open
    val newChannels = collection.mutable.Queue.empty[ChannelState]
    override def run() {
      for (i <- 0 until 32) {
        readBufferPool.put(ByteBuffer.allocateDirect(1048576))
      }
      while(true) {
        logger.trace("Reader Select started")
        // Register any pending write selectors
        val count = selector.select()
        newChannels.synchronized {
          if(!newChannels.isEmpty) {
            for(cs <- newChannels) {
              cs.channel.register(selector, SelectionKey.OP_READ, cs)
            }
            newChannels.clear()
          }
        }
        val it = selector.selectedKeys().iterator()
        //var totalBytesRead = 0
        while (it.hasNext) {
          val key = it.next()
          it.remove()

          // if the channel is ready to read bytes
          if (key.isValid && key.isReadable) {
            // Get the channel and buffer
            val channel = key.channel.asInstanceOf[SocketChannel]
            val state = key.attachment().asInstanceOf[ChannelState]

            // Do a read
            val bytesRead = channel.read(state.readBuffer)
            bytesReceivedMeter.mark(bytesRead)

            // Find the beginning of the first partial frame
            var startPosition = 0
            while (startPosition + 4 < state.readBuffer.position() &&
              startPosition + state.readBuffer.getInt(startPosition) + 4 <= state.readBuffer.position()) {
              startPosition += state.readBuffer.getInt(startPosition) + 4
            }

            // If we have read any complete frames split off the completed frames and restart the
            // buffer with the remainder
            if (startPosition > 0) {
              // Construct the finished buffer
              val finishedBuffer = state.readBuffer.duplicate()
              finishedBuffer.flip()
              finishedBuffer.limit(startPosition)
              processFrames(state.partitionId, finishedBuffer)
              // Construct the remainder buffer
              val remainderBuffer = state.readBuffer.duplicate()
              remainderBuffer.flip()
              remainderBuffer.position(startPosition)
              // Put the remainder in a new read buffer
              state.readBuffer = getNewReadBuffer(remainderBuffer.remaining())
              state.readBuffer.clear()
              state.readBuffer.put(remainderBuffer)
            }

            // Resize the read buffer if necessary (and possible)
            if (state.readBuffer.position() >= 4) {
              val frameSize = state.readBuffer.getInt(0)
              if (frameSize + 4 > state.readBuffer.capacity()) {
                val newBuffer = getNewReadBuffer(frameSize + 4)
                state.readBuffer.flip()
                newBuffer.put(state.readBuffer)
                state.readBuffer = newBuffer
              }
            }
          }
        }
      }
    }
  } // end of Reader

  var connections = new ConcurrentHashMap[NetworkDestinationHandle, ChannelState]
  val writerThread = new WriterThread
  private val connectionSemaphore = new Semaphore(0)

  // Initialize the read buffer pool with buffers
  val readBufferPool = new LinkedBlockingQueue[ByteBuffer]
  val readerThread = new ReaderThread
  val readExecutor = VeloxFixedThreadPool.pool

  val nextConnectionID = new AtomicInteger(0)

  override def setMessageService(messageService: MessageService) {
    this.messageService = messageService
  }

  def blockForConnections(numConnections: Integer) {
    connectionSemaphore.acquireUninterruptibly(numConnections)
  }

  def _registerConnection(partitionId: NetworkDestinationHandle, channelState: ChannelState) {
    // Register the connection and attach the selectors
    if (connections.putIfAbsent(partitionId, channelState) == null) {
      logger.info(s"Adding connection from $partitionId")
      val channel = channelState.channel
      channel.configureBlocking(false)
      channel.socket.setTcpNoDelay(tcpNoDelay)
      readerThread.newChannels.synchronized {
        readerThread.newChannels.enqueue(channelState)
      }
      writerThread.newChannels.synchronized {
        writerThread.newChannels.enqueue(channelState)
      }
      readerThread.selector.wakeup
      writerThread.selector.wakeup

      connectionSemaphore.release
    } else {
      logger.error("Already connected to " + partitionId)
    }
  }

  override def configureInboundListener(port: Integer) {
    val serverChannel = ServerSocketChannel.open()
    serverChannel.socket.bind(new InetSocketAddress(port))

    new Thread {
         override def run() {
           // Grab references to memebers in the parent class
           val connections = NIONetworkService.this.connections
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
             // Create a message reader thread to read the input buffer
             val socketThread = new ChannelState(connectionId, clientChannel)
             _registerConnection(connectionId, socketThread)
           }
         }
       }.start()
  }

  override def start() {
    // Start the messenger threads
    writerThread.start()
    readerThread.start()
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
        val socketThread = new ChannelState(handle, clientChannel)
        _registerConnection(handle, socketThread)
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

  override def send(dst: NetworkDestinationHandle, buffer: ByteBuffer) {
    assert(connections.containsKey(dst))
    //val bufferResized = connections.get(dst).writeMessage(buffer)
    // if (bufferResized) {
    //   writerThread.synchronized{ writerThread.notify() }
    // }
  }

  override def sendAny(buffer: ByteBuffer) {
    // if(connections.isEmpty) {
    //   logger.error("Empty connections list in sendAny!")
    // }

    // val connArray = connections.keySet.toArray
    // send(connArray(Random.nextInt(connArray.length)).asInstanceOf[NetworkDestinationHandle], buffer)
  }

  override def getConnections : Iterator[NetworkDestinationHandle] = {
    connections.keys
  }

  override def disconnect(which: NetworkDestinationHandle) {
    if(connections.contains(which)) {
      logger.error(s"Disconnection to $which requested, but $which not found!")
      return
    }

    connections.remove(which)
  }

  def receive(src: NetworkDestinationHandle, buffer: ByteBuffer) {
    bytesReadMeter.mark(buffer.remaining)
    messageService.receiveRemoteMessage(src, buffer)
  }


} // end of NIONetworkService
