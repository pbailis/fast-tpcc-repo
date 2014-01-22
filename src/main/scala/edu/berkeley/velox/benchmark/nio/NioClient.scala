package edu.berkeley.velox.benchmark.nio

import java.io.IOException

import java.nio.channels.SocketChannel
import java.nio.ByteBuffer
import java.net.InetSocketAddress

class ClientRunnable(addr: InetSocketAddress) extends Thread {
  override def run() {
    val chan = SocketChannel.open
    chan.connect(addr)

    val buf = ByteBuffer.allocateDirect(NioServer.bufferSize)

    for (i <- 0 until NioServer.bufferSize) buf.put( (i % 42).toByte )
    buf.flip

    try {
      while (true) {
        var wrote = 0
        while (wrote < NioServer.bufferSize)
          wrote += chan.write(buf)
        buf.rewind()
        chan.read(buf)
        buf.clear()
      }
    } catch {
      case e: Throwable => e.printStackTrace()
    }
  }
}

object NioClient {

  val usage = "NioClient HOST PORT NUM_THREADS"

  def main(args: Array[String]) {
    if (args.length < 3) {
      println(usage)
      System.exit(0)
    }
    val addr = new InetSocketAddress(args(0),args(1).toInt)
    val threads = args(2).toInt
    for(t <- 0 until threads) { new ClientRunnable(addr).start }
  }

}
