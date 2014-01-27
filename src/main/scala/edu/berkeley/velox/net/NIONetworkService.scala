package edu.berkeley.velox.net

import java.net.InetSocketAddress
import edu.berkeley.velox.conf.VeloxConfig
import java.nio.channels.{SelectionKey, SocketChannel, Selector, ServerSocketChannel}
import java.util.concurrent._
import edu.berkeley.velox.NetworkDestinationHandle
import java.io.{DataOutputStream, ByteArrayOutputStream, ByteArrayInputStream, DataInputStream}
import java.nio.ByteBuffer
import com.typesafe.scalalogging.slf4j.Logging
import scala.util.Random
import java.util.concurrent.atomic.AtomicInteger
import edu.berkeley.velox.rpc.MessageService

class NIONetworkService(val performIDHandshake: Boolean = false) extends NetworkService with Logging {

  class ChannelState (val partitionId: NetworkDestinationHandle,
                      val channel: SocketChannel,
                      val mb: Int = 16) {
    var isOpen = true;
    val writeBuffers = new RingBuffer(mb)
    val sizeBuffer = ByteBuffer.allocateDirect(4)

    var readSizeBuffer = ByteBuffer.allocateDirect(4)
    var readBuffer: ByteBuffer = null // ByteBuffer.allocateDirect(mb * 1048576)

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
      isOpen = false;
      channel.close();
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
            val buffer = state.writeBuffers
            // Test if there are any bytes remaining in the send buffer
            var hasRemaining = buffer.pending.exists(_.hasRemaining)
            // If the pending send buffer is empty try and get more to send
            if (!hasRemaining) {
              logger.trace("flipping send buffer")
              buffer.finishedSending()
              hasRemaining = buffer.pending.exists(_.hasRemaining)
            }
            // If there are bytes to send then try and send those bytes
            if (hasRemaining) {
              // write the bytes from the buffer
              var bytesWritten = channel.write(buffer.pending)
              var totalBytesWritten = bytesWritten

              while (bytesWritten > 0 && buffer.pending.exists(_.hasRemaining)) {
                bytesWritten = channel.write(buffer.pending)
                assert(bytesWritten >= 0)
                totalBytesWritten += bytesWritten
              }

              bytesWrittenOnRound += totalBytesWritten
              //println(s"Total bytes written: $totalBytesWritten")
              bytesSentMeter.mark(totalBytesWritten)
            }
          }
        } // end of while loop over iterator
        if(bytesWrittenOnRound == 0) {
          synchronized { wait(100) }
        }
      } // end of outer while loop
    } // end of run
  } // end of writer thread

  class ReaderThread extends Thread("Read-Selector-Thread") {
    val selector = Selector.open
    val newChannels = collection.mutable.Queue.empty[ChannelState]
    override def run() {
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
            /**
             * Step 1: Read the size of the read buffer if necessary
             */
            // If the read size buffer is not full then we
            // don't know the read buffer size
            if (state.readSizeBuffer.hasRemaining) {
              val bytesRead = channel.read(state.readSizeBuffer)
              assert(bytesRead >= 0)
              bytesReceivedMeter.mark(bytesRead)
              // If we read the size of the next buffer
              if (!state.readSizeBuffer.hasRemaining) {
                state.readSizeBuffer.flip
                val nextBufferSize = state.readSizeBuffer.getInt
                // If we need to resize the read buffer
                if (state.readBuffer == null || state.readBuffer.capacity < nextBufferSize) {
                  // initialize the new size and loop until big enough
                  // @todo make pretty
                  var newSize = if (state.readBuffer == null) 64 else state.readBuffer.capacity
                  while (nextBufferSize > newSize) {
                    newSize = 2 * newSize
                  }
                  state.readBuffer = ByteBuffer.allocateDirect(newSize)
                }
                // Set the limit on the read buffer
                state.readBuffer.clear()
                state.readBuffer.limit(nextBufferSize)
              }
            }

            /**
             * Step 2: If we know the size of the read buffer begin
             * (or continue) to fill it
             */
            if (!state.readSizeBuffer.hasRemaining) {
              val bytesRead = channel.read(state.readBuffer)
              assert(bytesRead >= 0)
              bytesReceivedMeter.mark(bytesRead)
            }

            /**
             * Step 3: Process the results of the read if necessary
             */
            // If the receive buffer is full process the result
            if (!state.readBuffer.hasRemaining) {
              // Process the read buffer (we make a local copy here)
              val oldReadBuffer = state.readBuffer
              val partitionId = state.partitionId
              readExecutor.execute(new Runnable {
                def run() {
                  oldReadBuffer.flip()
                  while (oldReadBuffer.hasRemaining) {
                    val msgSize = oldReadBuffer.getInt
                    assert(msgSize >= 0)
                    val bytes = new Array[Byte](msgSize)
                    oldReadBuffer.get(bytes)
                    NIONetworkService.this.receive(partitionId, bytes)
                  }
                  oldReadBuffer.clear()
                  readBufferPool.add(oldReadBuffer)
                }
              })
              // Get a new buffer (this could be null)
              state.readBuffer = readBufferPool.poll()
              // Reset the readSizeBuffer to allow for longer reads
              state.readSizeBuffer.clear()
            }
          }
        }
      }
    }
  } // end of Reader

  var connections = new ConcurrentHashMap[NetworkDestinationHandle, ChannelState]
  val writerThread = new WriterThread
  val readBufferPool = new ConcurrentLinkedQueue[ByteBuffer]
  val readerThread = new ReaderThread
  val readExecutor = Executors.newFixedThreadPool(16)
  val nextConnectionID = new AtomicInteger(0)

  override def setMessageService(messageService: MessageService) {
    this.messageService = messageService
  }

  def _registerConnection(partitionId: NetworkDestinationHandle, channelState: ChannelState) {
    // Register the connection and attach the selectors
    if (connections.putIfAbsent(partitionId, channelState) == null) {
      logger.debug(s"Adding connection from $partitionId")
      val channel = channelState.channel
      channel.configureBlocking(false)
      channel.socket.setTcpNoDelay(VeloxConfig.tcpNoDelay)
      readerThread.newChannels.synchronized {
        readerThread.newChannels.enqueue(channelState)
      }
      writerThread.newChannels.synchronized {
        writerThread.newChannels.enqueue(channelState)
      }
      readerThread.selector.wakeup()
      writerThread.selector.wakeup()
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
             clientChannel.socket.setTcpNoDelay(VeloxConfig.tcpNoDelay)
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
    val clientChannel = SocketChannel.open()
    clientChannel.connect(address)
    assert(clientChannel.isConnected)
    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)

    if(performIDHandshake) {
      dos.writeInt(VeloxConfig.partitionId)
      dos.flush()
      val bytes = bos.toByteArray()
      assert(bytes.size == 4)
      clientChannel.socket.getOutputStream.write(bytes)
    }
    val socketThread = new ChannelState(handle, clientChannel)
    _registerConnection(handle, socketThread)
  }

  override def connect(address: InetSocketAddress): NetworkDestinationHandle = {
    val handle = nextConnectionID.incrementAndGet()
    connect(handle, address)
    handle
  }

  override def send(dst: NetworkDestinationHandle, buffer: Array[Byte]) {
    assert(connections.containsKey(dst))
    val bufferResized = connections.get(dst).writeMessage(buffer)
    if (bufferResized) {
      writerThread.synchronized{ writerThread.notify() }
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

  def receive(src: NetworkDestinationHandle, buffer: Array[Byte]) {
    bytesReadMeter.mark(buffer.size)
    messageService.receiveRemoteMessage(src, buffer)
  }


} // end of NIONetworkService
