package edu.berkeley.velox.benchmark.nio

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.nio.channels.{ClosedByInterruptException, ClosedChannelException, SelectionKey
  , Selector, ServerSocketChannel, SocketChannel}
import java.util.concurrent.Executors
import scala.collection.immutable.HashSet
import scopt._

class StopThread(serv: ServerSocketChannel, selector: Selector, timeout: Int) extends Thread {

  override
  def run() {
    Thread.sleep(timeout)
    println("stopping")
    NioBenchmark.go = false
    serv.close
    selector.wakeup
  }
}

trait ReqRunner extends Runnable {
  var reqs = 0
  var bytes = 0l
  var doWrite = false
}

class NioRunnable(chan: SocketChannel, buf: ByteBuffer, selector: Selector, write: Boolean) extends ReqRunner {

  doWrite = write
  var selKey: SelectionKey = null

  def run() = this.synchronized { // must be synchronized, we might get re-called by selector too soon
    try {
      if (doWrite) {
        val wrote = chan.write(buf)
        if (buf.remaining == 0) {
          doWrite = false
          buf.clear
          selKey.interestOps(SelectionKey.OP_READ)
        } else
          selKey.interestOps(SelectionKey.OP_WRITE)
        selector.wakeup
      } else {
        val read = chan.read(buf)
        bytes += read
        if (buf.remaining == 0) {
          reqs += 1
          doWrite = true
          buf.rewind
          selKey.interestOps(SelectionKey.OP_WRITE)
        } else
          selKey.interestOps(SelectionKey.OP_READ)
        selector.wakeup
      }
    } catch {
      case e: ClosedChannelException => {}
      case e: ClosedByInterruptException => {}
      case t: Throwable => throw t
    }
  }

}

class BufferingNioRunnable(chan: SocketChannel, bufs: Array[ByteBuffer], selector: Selector,
                           write: Boolean, simCopy: Boolean) extends ReqRunner {

  doWrite = write
  var selKey: SelectionKey = null

  val bigBuf =
    if (simCopy) ByteBuffer.allocateDirect(bufs(0).limit * bufs.length)
    else null

  def run() = this.synchronized { // must be synchronized, we might get re-called by selector too soon
    try {
      if (simCopy && doWrite) {
        if (bufs(0).position == 0) // simulate copying a bunch of buffers into one big buffer
          gatherBufs
        val wrote = chan.write(bigBuf)
        if (bigBuf.remaining == 0) {
          doWrite = false
          bufs.foreach(_.clear)
          selKey.interestOps(SelectionKey.OP_READ)
        } else
          selKey.interestOps(SelectionKey.OP_WRITE)
      }
      else if (doWrite) {
        val wrote = chan.write(bufs)
        if (bufs(bufs.length -1).remaining == 0) {
          doWrite = false
          bufs.foreach(_.clear)
          selKey.interestOps(SelectionKey.OP_READ)
        } else
          selKey.interestOps(SelectionKey.OP_WRITE)
      } else {
        val read = chan.read(bufs)
        bytes += read
        if (bufs(bufs.length-1).remaining == 0) {
          reqs += bufs.length
          doWrite = true
          bufs.foreach(_.rewind)
          selKey.interestOps(SelectionKey.OP_WRITE)
        } else
          selKey.interestOps(SelectionKey.OP_READ)
      }
      selector.wakeup
    } catch {
      case e: ClosedChannelException => {}
      case e: ClosedByInterruptException => {}
      case t: Throwable => throw t
    }
  }

  def gatherBufs() {
    bigBuf.clear
    bufs.map(b=>bigBuf.put(b))
    bigBuf.flip
  }

}

object NioBenchmark {

  case class Config(port: Int = 8080, secs: Int = 20, threads: Int = 3,
    reqsize: Int = 64, client: Boolean = false, array: Int = 0, direct: Boolean = true,
    simCopy: Boolean = false,
    host: String = "localhost", processBytes: Boolean = false, nonBlocking: Boolean = true)


  val nanospersec = 1000000000l

  var go = true

  val parser = new scopt.OptionParser[Config]("NioTester") {
    head("NioTester", "0.1")

    opt[Int]('p',"port") action { (x, c) =>
      c.copy(port = x) } text("Port to connect to / listen on")
    opt[Int]('s',"secs") action { (x, c) =>
      c.copy(secs = x) } text("Seconds to run the test")
    opt[Int]('t',"threads") action { (x, c) =>
      c.copy(threads = x) } text("Number of threads to use")
    opt[Int]('r',"reqSize") action { (x, c) =>
      c.copy(reqsize = x) } text("Size of each request (in bytes)")

    opt[Int]('a',"array") optional() action { (x, c) =>
      c.copy(array = x) } text ("Use an array of byte buffers.  Argument is what length of array to use")
    opt[Boolean]('C',"copy") optional() action { (x, c) =>
      c.copy(simCopy = x) } text ("Copy data out of small buffers into one big buffer (no effect unless --array also specified)")
    opt[Boolean]('d',"direct") optional() action { (x, c) =>
      c.copy(direct = x) } text ("Use allocateDirect to allocate ByteBuffers")
    opt[Boolean]('c',"client") optional() action { (x, c) =>
      c.copy(client = x) } text ("run in client mode (send before receive)")
    opt[String]('h',"host") optional() action { (x, c) =>
      c.copy(host = x) } text("host to connect to")
    opt[Boolean]('b',"processBytes") optional() action { (x, c) =>
      c.copy(processBytes = x) } text("Ensure received bytes are correct")
    opt[Boolean]('n',"nonBlocking") optional() action { (x, c) =>
      c.copy(nonBlocking = x) } text("Run in non-blocking mode")

    help("help") text("prints this usage text")
  }


  def runNonblockingServer(config: Config) {
    val executor =  Executors.newFixedThreadPool(config.threads)
    val selector = Selector.open

    var first = !config.client
    var start: Long = 0l


    var serv: ServerSocketChannel = null
    var skey: SelectionKey = null

    var runnables = new HashSet[ReqRunner]

    if (config.client) {
      for (i <- 0 until config.threads) {
        val channel = SocketChannel.open
        channel.configureBlocking(false)
        //channel.setOption(StandardSocketOptions.TCP_NODELAY.asInstanceOf[SocketOption[Any]],true)
        channel.register(selector,SelectionKey.OP_CONNECT,channel)
        channel.connect(new InetSocketAddress(config.host,config.port))
      }
      println("Starting in client mode")
    } else {
      serv = ServerSocketChannel.open
      serv.configureBlocking(false)
      skey = serv.register(selector,SelectionKey.OP_ACCEPT)
      serv.bind(new InetSocketAddress(config.port))
      println("Starting in server mode")
    }

    while (go) {
      val selkeys = selector.select
      //println(s"selected $selkeys keys")
      if (selkeys > 0) {

        if (first) {
          start = System.nanoTime
          new StopThread(serv,selector,config.secs * 1000).start
          println("running")
          first = false
        }

        val it = selector.selectedKeys.iterator

        while(it.hasNext()) {
          val key = it.next

          if (key.isValid) {

            if (key == skey && key.isAcceptable) {
              val channel = serv.accept()
              if (config.array == 0) {
                val buf =
                  if (config.direct) ByteBuffer.allocateDirect(config.reqsize)
                  else               ByteBuffer.allocate(config.reqsize)
                channel.configureBlocking(false)
                //channel.setOption(StandardSocketOptions.TCP_NODELAY.asInstanceOf[SocketOption[Any]],true)
                val nioRunner = new NioRunnable(channel,buf,selector,write=false)
                runnables += nioRunner
                val niokey = channel.register(selector, SelectionKey.OP_READ, nioRunner)
                nioRunner.selKey = niokey
              } else {
                val barray = new Array[ByteBuffer](config.array).map(_=>
                  if (config.direct) ByteBuffer.allocateDirect(config.reqsize)
                  else               ByteBuffer.allocate(config.reqsize))
                channel.configureBlocking(false)
                val nioRunner = new BufferingNioRunnable(channel,barray,selector,write=false,simCopy=config.simCopy)
                runnables += nioRunner
                val niokey = channel.register(selector, SelectionKey.OP_READ, nioRunner)
                nioRunner.selKey = niokey
              }
            }

            else if (key.isConnectable) {
              val channel = key.attachment.asInstanceOf[SocketChannel]
              channel.finishConnect
              val runner =
                if (config.array == 0) {
                  val buf =
                    if (config.direct) ByteBuffer.allocateDirect(config.reqsize)
                    else               ByteBuffer.allocate(config.reqsize)
                  for (i <- (1 to config.reqsize))
                    buf.put((i%127).toByte)
                  buf.flip
                  val nioRunner = new NioRunnable(channel,buf,selector,write=true)
                  runnables += nioRunner
                  nioRunner.selKey = key
                  nioRunner
                } else {
                  val barray = new Array[ByteBuffer](config.array).map(_=>{
                    val bb =
                      if (config.direct) ByteBuffer.allocateDirect(config.reqsize)
                      else               ByteBuffer.allocate(config.reqsize)
                    for (i <- (1 to config.reqsize))
                      bb.put((i%127).toByte)
                    bb.flip
                    bb
                  })
                  val nioRunner = new BufferingNioRunnable(channel,barray,selector,write=true,simCopy=config.simCopy)
                  runnables += nioRunner
                  nioRunner.selKey = key
                  nioRunner
                }

              key.interestOps(SelectionKey.OP_WRITE)
              key.attach(runner)
            }

            else if (key.isReadable) {
              key.interestOps(0)
              val nioRunner = key.attachment.asInstanceOf[ReqRunner]
              if (!nioRunner.doWrite) {
                executor.execute(nioRunner)
              }
            }

            else if (key.isWritable) {
              key.interestOps(0)
              val nioRunner = key.attachment.asInstanceOf[ReqRunner]
              if (nioRunner.doWrite) {
                executor.execute(nioRunner)
              }
            }

          }

          it.remove
        }
      }
    }
    println("done")

    executor.shutdownNow

    val reqs = runnables.foldLeft(0)(_ + _.reqs)
    val bytes = runnables.foldLeft(0l)(_ + _.bytes)

    if (executor.awaitTermination(10,java.util.concurrent.TimeUnit.SECONDS)) {
       val secs = (System.nanoTime-start).toDouble/nanospersec
      // val bufs = totalReqs.get
      // val bufspersec = bufs/secs
      // val reqs = bufs * reqstobuf
      val reqspersec = reqs/secs
      val bytesPerSec = (bytes / 1000000.0) / secs

      println(s"Received $reqs requests in $secs seconds")
      // println(s"$bufspersec buffers / second")
      println(s"$reqspersec requests / second")
      println(s"$bytesPerSec MByte / second")
      println("OKAY")
    } else {
      println("Tasks won't finish execution")
    }
  }

  def main(args: Array[String]) {
    parser.parse(args, Config()) map { config =>
      if (config.nonBlocking)
        runNonblockingServer(config)
      else
        println("Not implemented yet")
    } getOrElse {
      println("Invalid arguments")
    }
  }

}
