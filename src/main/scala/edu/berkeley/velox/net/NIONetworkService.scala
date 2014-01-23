package edu.berkeley.velox.net

import java.net.InetSocketAddress
import edu.berkeley.velox.conf.VeloxConfig
import java.nio.channels.{SelectionKey, SocketChannel, Selector, ServerSocketChannel}
import java.util.concurrent.{Semaphore, Executors, ConcurrentHashMap}
import edu.berkeley.velox.PartitionId
import java.io.{DataOutputStream, ByteArrayOutputStream, ByteArrayInputStream, DataInputStream}
import edu.berkeley.velox.rpc.MessageService
import java.util.concurrent.atomic.{AtomicInteger, AtomicBoolean}
import java.util.Map.Entry
import java.util.concurrent.locks.ReentrantLock


class NIONetworkService extends NetworkService {

  /**
   * Loops on the socket buffer reading byte messages
   * and passing them into the messaging layer.
   *
   * @param partitionId
   * @param mb
   */
  class SocketThread(val partitionId: PartitionId,
                     val channel: SocketChannel,
                     val mb: Int = 16) extends Thread(s"Socket-Thread-$partitionId") {
    val readBuffer = new DoubleBuffer(2*mb)
    val writeBuffer = new DoubleBuffer(mb)
    val readyToWrite = new AtomicBoolean(false)
    val writerLock = new Semaphore(1)
    // Initialize the active read buffer and pending write buffer to empty
    readBuffer.active.flip() // Nothing to read yet
    writeBuffer.pending.flip()  // Nothing to send ye
    // Disable negel
    channel.socket.setTcpNoDelay(VeloxConfig.tcpNoDelay)
    override def run() {
      while (true) {
        // Get the next message
        val nextMessage = readBuffer.getMessage
        NIONetworkService.this.recv(partitionId, nextMessage)
      }
    }
    def writeMessage(bytes: Array[Byte]) {
      writeBuffer.writeMessage(bytes)
      msgSentCounter.incrementAndGet()
      bytesWrittenCounter.addAndGet(bytes.size)
      readyToWrite.set(true)
    }
  } // end of SocketReader


  /**
   * A single writer thread multiplexes writing on each socket
   */
  class WriterThread extends Thread("Write-Selector-Thread") {
    val requestExecutor = Executors.newFixedThreadPool(4)
    override def run() {
      while(true) {
        //println("Starting Writes")
        val connections = NIONetworkService.this.connections
        val iter = connections.entrySet.iterator
        while (iter.hasNext) {
          val pair = iter.next
          val socketThread = pair.getValue
          if (socketThread.writerLock.tryAcquire) {
            if (socketThread.readyToWrite.get()) {
              socketThread.readyToWrite.set(false)
              requestExecutor.execute(new Runnable {
                def run() {
                  // Get the channel
                  val channel = socketThread.channel.asInstanceOf[SocketChannel]
                  val buffer = socketThread.writeBuffer
                  // If the pending send buffer is empty try and get more to send
                  if (!buffer.pending.hasRemaining) {
                    buffer.finishedSending()
                    //println("flipping send buffer")
                  }
                  // If there are bytes to send then try and send those bytes
                  if (buffer.pending.hasRemaining) {
                    // write the bytes from the buffer
                    var bytesWritten = 0
                    while (buffer.pending.hasRemaining) {
                      val result = channel.write(buffer.pending)
                      assert(result >= 0)
                      bytesWritten += result
                    }
                    //println(s"Bytes written: $bytesWritten")
                    // println(s"Sent $bytesWritten bytes.")
                    // ensure not end of stream
                    assert(bytesWritten > 0)
                    bytesSentCounter.addAndGet(bytesWritten)
                  }
                  socketThread.writerLock.release

                  synchronized { notify() }
                }
              }) // End of runnable
            } else { // if not ready to write then we can release the lock
              socketThread.writerLock.release
            }
          } // end of trylock
        } // end of while loop over iterator
        synchronized { wait(10) }
      } // end of outer while loop
    } // end of run
  } // end of writer thread



  class ReaderThread extends Thread("Read-Selector-Thread") {
    val selector = Selector.open
    val newChannels = collection.mutable.Queue.empty[SocketThread]
    override def run() {
      while(true) {
        // println("Reader Select started")
        val count = selector.select()
        newChannels.synchronized {
          if(!newChannels.isEmpty) {
            for(s <- newChannels) {
              s.channel.register(selector, SelectionKey.OP_READ, s.readBuffer)
            }
            newChannels.clear
          }
        }
        val it = selector.selectedKeys().iterator()
        var totalBytesRead = 0
        while (it.hasNext) {
          val key = it.next()
          it.remove()
          // if the channel is ready to read bytes
          if (key.isValid && key.isReadable) {
            // Get the channel
            val channel = key.channel.asInstanceOf[SocketChannel]
            val buffer = key.attachment().asInstanceOf[DoubleBuffer]
            buffer.pendingLock.synchronized {
              // If there is no space left in the read buffer then double it
              if (!buffer.pending.hasRemaining) {
                buffer.doublePending
              }
              // Read the bytes
              assert(buffer.pending.hasRemaining)
              val bytesRead = channel.read(buffer.pending)
              bytesRecvCounter.addAndGet(bytesRead)
              assert(bytesRead >= 0)
            }
            totalBytesRead += 1
            // Notify any waiting threads that we are finished reading
            buffer.finishedReading()
          }
        }
      }
    }
  } // end of Reader


  var connections = new ConcurrentHashMap[PartitionId, SocketThread]
  val writerThread = new WriterThread
  val readerThread = new ReaderThread


  override def setMessageService(messageService: MessageService) {
    this.messageService = messageService
    this.messageService.networkService = this
  }

  def addConnection(partitionId: PartitionId, socketThread: SocketThread) {
    // Register the connection and attach the selectors
    if (connections.putIfAbsent(partitionId, socketThread) == null) {
      println(s"Adding connection from $partitionId")
      val channel = socketThread.channel
      channel.configureBlocking(false)
      readerThread.newChannels.synchronized {
        readerThread.newChannels.enqueue(socketThread)
      }
      readerThread.selector.wakeup()
      socketThread.start()
    } else {
      println("Already connected to " + partitionId)
    }
  }

  def start() {
    val serverChannel = ServerSocketChannel.open()
    serverChannel.socket.bind(new InetSocketAddress(VeloxConfig.serverPort))
    // Start the messenger threads
    writerThread.start()
    readerThread.start()
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
          val bytes = new Array[Byte](4)
          val bytesRead = clientChannel.socket.getInputStream.read(bytes)
          assert(bytesRead == 4)
          // Read the partition id
          val partitionId: PartitionId =
            new DataInputStream(new ByteArrayInputStream(bytes)).readInt()
          // Create a message reader thread to read the input buffer
          val socketThread = new SocketThread(partitionId, clientChannel)
          addConnection(partitionId, socketThread)
        }
      }
    }.start()

    // Wait for connections
    Thread.sleep(VeloxConfig.bootstrapConnectionWaitSeconds * 1000)

    // connect to all higher-numbered partitions
    VeloxConfig.serverAddresses.filter {
      case (id, addr) => id > VeloxConfig.partitionId
    }.foreach {
      case (partitionId, remoteAddress) =>
        val clientChannel = SocketChannel.open()
        clientChannel.connect(remoteAddress)
        assert(clientChannel.isConnected)
        //println("Starting Connection")
        val bos = new ByteArrayOutputStream()
        val dos = new DataOutputStream(bos)
        dos.writeInt(VeloxConfig.partitionId)
        dos.flush()
        val bytes = bos.toByteArray()
        assert(bytes.size == 4)
        clientChannel.socket.getOutputStream.write(bytes)
        val socketThread = new SocketThread(partitionId, clientChannel)
        addConnection(partitionId, socketThread)
    }
    Thread.sleep(VeloxConfig.bootstrapConnectionWaitSeconds * 1000)
  }

  def send(dst: PartitionId, buffer: Array[Byte]) {
    //println(s"Sending ${VeloxConfig.partitionId} --> $dst")
    assert(connections.containsKey(dst))
    connections.get(dst).writeMessage(buffer)
    writerThread.synchronized{ writerThread.notify() }
  }


  def recv(src: PartitionId, buffer: Array[Byte]) {
    bytesReadCounter.addAndGet(buffer.size)
    messageService.receiveRemoteMessage(src, buffer)
  }


} // end of NIONetworkService
