package edu.berkeley.velox.benchmark.nio

import java.net.InetSocketAddress

import java.nio.channels._
import java.nio.ByteBuffer

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.Executors
import java.util.{Timer,TimerTask}


class RequestExecutor(channel: SocketChannel) extends Runnable {

  def process(buf: ByteBuffer): Unit = {
    buf.flip
    for(i <- 0 until NioServer.bufferSize) {
      val x = buf.get
      if (x != (i % 42).toByte) {
        println("Got invalid data!")
        System.exit(-1)
      }
    }
  }

  def run() {
    val buf = ByteBuffer.allocateDirect(NioServer.bufferSize)
    var reqs = 0
    var bytes = 0

    try {
        var read = 0
        while (read < NioServer.bufferSize)
          read += channel.read(buf)
        if (NioServer.processBytes) process(buf)
        reqs += 1
        bytes += read
        buf.rewind()
        channel.write(buf)
        buf.clear()
    } catch {
      case e: Throwable => ()
    }

    NioServer.totalReqs.addAndGet(reqs)
    NioServer.totalBytes.addAndGet(bytes)
  }

}

object NioServer {

  val selector = Selector.open

  // use a multiple of 4
  val bufferSize = 64 * 100

  val totalReqs = new AtomicInteger(0)
  val totalBytes = new AtomicInteger(0)

  var go = true

  val nanospersec = 1000000000l

  val usage = "NioServer PORT SECONDS_TO_RUN [--processBytes]"

  var processBytes = false

  def processArgs(args: Array[String]): Array[String] =  {
    args.filter(_ match {
      case "--processBytes" => {
        processBytes = true
        false
      }
      case _ => true
    })
  }

  def main(args: Array[String]) {

    val rargs = processArgs(args)

    if (rargs.length < 2) {
      println(usage)
      System.exit(0)
    }


    val serv = ServerSocketChannel.open
    serv.socket.bind(new InetSocketAddress(rargs(0).toInt))
    val secondsToRun = rargs(1).toInt
    serv.configureBlocking(false)
    val serverKey = serv.register(selector, serv.validOps())

    val executor = Executors.newFixedThreadPool(16)

    println("waiting for connections")

    var start = 0l
    val timer = new Timer
    var first = true

    while (go) {
      try {
        selector.select

        val it = selector.selectedKeys().iterator()
        while(it.hasNext) {
           val sk = it.next()

          if(sk == serverKey && sk.isValid && sk.isAcceptable) {

            val channel = serv.accept()
             if (first) {
               start = System.nanoTime
               timer.schedule(
                 new TimerTask() { def run() { serv.close; NioServer.go = false } },
                 secondsToRun * 1000
               )
               first = false
             }

            if(channel != null) {
             channel.configureBlocking(false)

              channel.register(selector, channel.validOps())
          }
          }
          else if(sk.isValid && sk.isReadable) {
            executor.execute(new RequestExecutor(sk.channel.asInstanceOf[SocketChannel]))
        }
        }
      } catch {
        case e: Throwable => println(e.getMessage); e.printStackTrace()
      }
    }

    timer.cancel
    executor.shutdownNow

    if (executor.awaitTermination(10,java.util.concurrent.TimeUnit.SECONDS)) {
      val secs = (System.nanoTime-start).toDouble / nanospersec
      val reqs = totalReqs.get
      val persec = reqs / secs
      val bytesPerSec = (totalBytes.get / 1000000.0) / secs

      println(s"Received $reqs requests in $secs seconds")
      println(s"$persec requests / second")
      println(s"$bytesPerSec MByte / second")
    } else {
      println("Tasks won't finish execution")
    }

  }

}
