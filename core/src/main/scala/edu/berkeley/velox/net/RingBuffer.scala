package edu.berkeley.velox.net

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock
import edu.berkeley.velox.conf.VeloxConfig

/**
 * This class models an active and pending buffer in which the user
 * thread is either reading or writing to the active buffer and the
 * network layer is writing or reading to the pending buffer.  When
 * possible the network layer will try to swtich buffers
 */
class RingBuffer(val mb: Int = 1) {
  val MByte = 1048576

  val poolSize = VeloxConfig.numBuffersPerRing

  val activeIndex = new AtomicInteger(0)
  var active = Array.fill(poolSize){ ByteBuffer.allocateDirect(mb * MByte) }
  active(0).putInt(-1)
  var bufferLocks = Array.fill(poolSize){ new Integer(0) }

  var pending = Array.fill(poolSize){
    val b = ByteBuffer.allocateDirect(mb * MByte)
    b.flip()
    b
  }

  /**
   * Write the byte array into the active buffer
   * @param bytes
   */
  def writeMessage(bytes: Array[Byte]): Boolean = {
    // Grab the read lock on the active buffer pool so the sending thread cannot swap
    val bufferIndex = activeIndex.getAndIncrement % poolSize
    val lock: Integer = bufferLocks(bufferIndex)
    var bufferResized = false
    lock.synchronized {
      val msgLen = 4 + bytes.size
      // Resize the buffer if necessary
      if(msgLen > active(bufferIndex).remaining()) {
        // Double the buffer size until capacity is met
        var newSize = active(bufferIndex).capacity
        while (msgLen > newSize - active(bufferIndex).position()) {
          newSize = newSize * 2
          println(s"Doubling sending buffer: ${newSize.toDouble / MByte}")
        }
        // allocate the new buffer keeping a copy of the old buffer
        val oldActive = active(bufferIndex)
        active(bufferIndex) = ByteBuffer.allocateDirect(newSize)
        // write the contents of the old buffer into the new buffer
        oldActive.flip()
        active(bufferIndex).put(oldActive)
        assert(msgLen <= active(bufferIndex).remaining)
        bufferResized = true
      }
      // Write the message length and content
      active(bufferIndex).putInt(bytes.size)
      active(bufferIndex).put(bytes)
      //println(s"wrote to buffer $active")
    }
    bufferResized
  }

  def finishedSending() {
    // Swap active and pending buffers one at a time
    for (i <- 0 until active.size) {
      val lock = bufferLocks(i)
      lock.synchronized {
        val tmp = active(i)
        active(i) = pending(i)
        active(i).clear()
        pending(i) = tmp
        if (i == 0) {
          active(i).putInt(-1)
        }
      }
    }
    // Update the frames size for each of the pending buffers
    val totalLength = pending.foldLeft(0)((sum, b) => (sum + b.position)) - 4
    if (totalLength > 4) {
      pending(0).putInt(0,totalLength)
    } else {
      pending(0).clear()
    }
    pending.foreach(b => b.flip())
  }

} // End of Double Buffer


