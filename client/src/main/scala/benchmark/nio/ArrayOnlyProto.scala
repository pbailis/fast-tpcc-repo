package edu.berkeley.velox.benchmark.nio

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{ClosedByInterruptException, ClosedChannelException, SelectionKey, Selector, ServerSocketChannel, SocketChannel}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable.HashSet
import scopt._
import edu.berkeley.velox.util.VeloxFixedThreadPool

class SocketBuffer(channel: SocketChannel) extends Runnable {
  var buf = new Array[Byte](ArrayOnlyProto.bufSize)

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
    val wrote = channel.write(ByteBuffer.wrap(buf,0,writePos.get))
    //println(s"send: ${wrote}")

    // reset write position
    writePos.set(4)

    sending.set(false)
  }

}

// class SocketManager(channel: SocketChannel) {
//   // buffers that keep outgoing bytes
//   var currentBuf = new SocketBuffer
//   var pendingBuf = new SocketBuffer

//   /**
//     * Swap the buffers if unable to write len bytes
//     *
//     * We check if we can write so if multiple threads call swap
//     * only the first one will actually swap things
//     */
//   def swap(len: Int): Unit = this.synchronized {
//     if (currentBuf.writePos.get + len > currentBuf.buf.length) {

//       // This is bad, we want to swap already and we haven't sent our
//       // old buffer yet.  For now, spin wait
//       while (pendingBuf.sending.get) Thread.sleep(5)
//       val t = currentBuf
//       currentBuf = pendingBuf
//       pendingBuf = t

//     }
//   }

//   def write(bytes: Array[Byte]) {
//     while (!currentBuf.write(bytes))
//       swap(bytes.length)
//   }


// }

class Sender(channel: SocketChannel, var reqs: Int) extends Runnable {
  def run() {
    val bytes = new Array[Byte](ArrayOnlyProto.reqSize)
    while (reqs > 0) {
      ArrayOnlyProto.service.send(channel,bytes)
      reqs-=1
    }
  }
}

class Receiver(bytes: ByteBuffer, executor: ExecutorService, channel: SocketChannel) extends Runnable {
  def run() {
    val total = bytes.getInt
    var reqs = 0
    while(bytes.remaining != 0) {
      val msgLen = bytes.getInt

      // todo: actually process here
      bytes.position(bytes.position+msgLen)
      reqs+=1
    }
    ArrayOnlyProto.totalReqs.addAndGet(reqs)
    executor.submit(new Sender(channel,reqs))
  }
}

class ProtoService(executor: ExecutorService) {

  val writeExecutor = VeloxFixedThreadPool.pool

  val channelMap = new ConcurrentHashMap[SocketChannel, SocketBuffer]

  /**
    * Register a channel to be managed by this service
    *
    * This will start a thread to read from the channel
    * and allow sends to this channel through the service
    */
  def registerChannel(channel: SocketChannel) {
    val buf = new SocketBuffer(channel)
    channelMap.put(channel,buf)

    // start up a read thread
    new Thread() {
      val sizeBuffer = ByteBuffer.allocate(4)
      override def run() {
        while(true) {
          channel.read(sizeBuffer)
          sizeBuffer.flip
          val len = sizeBuffer.getInt
          sizeBuffer.rewind
          val readBuffer = ByteBuffer.allocate(len)
          channel.read(readBuffer)
          readBuffer.flip
          executor.submit(new Receiver(readBuffer,executor,channel))
        }
      }
    }.start
  }

  def send(dst: SocketChannel, buffer: Array[Byte]) {
    val sockBuf = channelMap.get(dst)

    // TODO: Something if buffer is null

    while (!sockBuf.write(buffer)) {
      // If we can't write, try to send
      if (sockBuf.sending.compareAndSet(false,true)) {
        //println("submmiting send")
        writeExecutor.submit(sockBuf)
        //sockBuf.run
      }
      Thread.sleep(5)
    }
  }

}

object ArrayOnlyProto {

  val totalReqs = new AtomicInteger(0)
  var service: ProtoService = null

  case class Config(port: Int = 8080, secs: Int = 20, threads: Int = 3,
    reqsize: Int = 8, bufsize: Int = 8000, initialRequestCount: Int = 1000,
    client: Boolean = false, host: String = "localhost")


  val nanospersec = 1000000000l
  var bufSize = 8000
  var reqSize = 8

  var go = true

  val parser = new scopt.OptionParser[Config]("ArrayOnlyProto") {
    head("ArrayOnlyProto", "0.1")

    opt[Int]('p',"port") action { (x, c) =>
      c.copy(port = x) } text("Port to connect to / listen on")
    opt[Int]('s',"secs") action { (x, c) =>
      c.copy(secs = x) } text("Seconds to run the test")
    opt[Int]('t',"threads") action { (x, c) =>
      c.copy(threads = x) } text("Number of threads to use")
    opt[Int]('r',"reqSize") action { (x, c) =>
      c.copy(reqsize = x) } text("Size of each request (in bytes)")

    opt[Int]('i', "initReqs") optional() action { (x, c) =>
      c.copy(initialRequestCount = x) } text("How many requests the client should send initially")
    opt[Int]('b',"bufSize") optional() action { (x, c) =>
      c.copy(bufsize = x) } text("Size of buffer array (in bytes)")
    opt[Boolean]('c',"client") optional() action { (x, c) =>
      c.copy(client = x) } text ("run in client mode (send before receive)")
    opt[String]('h',"host") optional() action { (x, c) =>
      c.copy(host = x) } text("host to connect to")

    help("help") text("prints this usage text")
  }


  def getChannel(config: Config): SocketChannel = {
    if (config.client) {
      val channel = SocketChannel.open
      println("Connecting as client")
      channel.connect(new InetSocketAddress(config.host,config.port))
      channel
    } else {
      val serv = ServerSocketChannel.open
      serv.socket.bind(new InetSocketAddress(config.port))
      println("Waiting for connection")
      serv.accept()
    }
  }



  def runTest(config: Config, channel: SocketChannel) {
    //val executor =  Executors.newFixedThreadPool(config.threads)
    val executor =  Executors.newCachedThreadPool()

    service = new ProtoService(executor)
    service.registerChannel(channel)

    if (config.client)
      new Thread(new Sender(channel,config.initialRequestCount)).start

    println("starting")
    val starttime = System.nanoTime
    try {
      Thread.sleep(config.secs * 1000)
    } catch {
      case t: Throwable => {
        println("Oops, something happened")
        t.printStackTrace
        println("Probably ignore results")
      }
    }

    val secs = (System.nanoTime - starttime).toDouble/nanospersec

    executor.shutdownNow
    if (executor.awaitTermination(10,java.util.concurrent.TimeUnit.SECONDS)) {
      // val secs = (System.nanoTime-start).toDouble/nanospersec
      // val bufs = totalReqs.get
      // val bufspersec = bufs/secs
      // val reqs = bufs * reqstobuf
      val reqs = totalReqs.get
      val reqspersec = reqs/secs
      //val bytesPerSec = (bytes / 1000000.0) / secs

      println(s"Received $reqs requests in $secs seconds")
      // println(s"$bufspersec buffers / second")
      println(s"$reqspersec requests / second")
      //println(s"$bytesPerSec MByte / second")
      println("OKAY")
    } else {
      println("Tasks won't finish execution")
    }
  }


  def main(args: Array[String]) {
    parser.parse(args, Config()) map { config =>
      reqSize = config.reqsize
      bufSize = config.bufsize
      val channel = getChannel(config)
      runTest(config,channel)
    }
  }

}
