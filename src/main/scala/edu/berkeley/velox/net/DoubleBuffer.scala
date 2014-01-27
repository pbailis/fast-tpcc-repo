package edu.berkeley.velox.net

import java.nio.ByteBuffer



/**
 * This class models an active and pending buffer in which the user
 * thread is either reading or writing to the active buffer and the
 * network layer is writing or reading to the pending buffer.  When
 * possible the network layer will try to swtich buffers
 */
class DoubleBuffer(val mb: Int = 1) {
  val MByte = 10485760

  var active = ByteBuffer.allocateDirect(mb * MByte)
  val pendingLock: Integer = 0
  var pending = ByteBuffer.allocateDirect(mb * MByte)

  def doublePending {
    println("Double read buffer.")
    val oldPending = pending
    pending = ByteBuffer.allocateDirect(oldPending.capacity * 2)
    oldPending.flip()
    pending.put(oldPending)
  }

  /**
   * Write the byte array into the active buffer
   * @param bytes
   */
  def writeMessage(bytes: Array[Byte]) {
    this.synchronized {
      val msgLen = 4 + bytes.size
      // Resize the buffer if necessary
      if(msgLen > active.remaining()) {
        // Double the buffer size until capacity is met
        var newSize = active.capacity
        while (msgLen > newSize - active.position()) {
          println("Doubling sending buffer.")
          newSize = newSize * 2
        }
        // allocate the new buffer keeping a copy of the old buffer
        val oldActive = active
        active = ByteBuffer.allocate(newSize)
        // write the contents of the old buffer into the new buffer
        oldActive.flip()
        active.put(oldActive)
        assert(msgLen <= active.remaining)
      }
      // Write the message length and content
      // active.putInt(bytes.size)
      active.put((bytes.size & 0xFF).toByte)
      active.put(((bytes.size >>> 8) & 0xFF).toByte)
      active.put(((bytes.size >>> 16) & 0xFF).toByte)
      active.put(((bytes.size >>> 24) & 0xFF).toByte)
      // Write the message content
      active.put(bytes)
      //println(s"wrote to buffer $active")
    }
  }

  /**
   * Blocking get message routine reads the next message off the active buffer.
   *
   * @return
   */
  def getMessage: Array[Byte] = {
    val msgLen = getInt()
    assert(msgLen >= 0)
    val msgBody = new Array[Byte](msgLen)
    get(msgBody)
    msgBody
  }

  /**
   * Get the next int off the active buffer
   * @return
   */
  def getInt(): Int = {
    val b = new Array[Byte](4)
    get(b)
    (b(0).toInt & 0xFF) | ((b(1).toInt & 0xFF) << 8)| ((b(2).toInt & 0xFF) << 16) | ((b(3).toInt & 0xFF) << 24)
  }

  /**
   * Fill the byte array from the active buffer
   *
   * @param bytes
   */
  def get(bytes: Array[Byte]) {
    synchronized {
      var totalBytesRead = 0
      while (totalBytesRead < bytes.size) {
        // Determine how much we can read on the next read
        var nextRead = math.min(active.remaining, bytes.size - totalBytesRead)
        // Try and switch over the buffer
        if (nextRead == 0) { finishedReadingUnsync() }
        nextRead = math.min(active.remaining, bytes.size - totalBytesRead)
        if (nextRead == 0) {
          wait() // The buffer is empty so wait for more data
        } else {
          active.get(bytes, totalBytesRead, nextRead)
          totalBytesRead += nextRead
        }
      }
    }
  }

  def finishedSending() {
    this.synchronized {
      val tmp = active
      active = pending
      pending = tmp
      // Reset the active buffer
      active.clear()
      // Flip the pending buffer to enable sending
      pending.flip()
    }
  }

  def finishedReadingUnsync() {
    pendingLock.synchronized {
      // Only if the active buffer is done we can flip
      if (!active.hasRemaining) {
        val tmp = active
        active = pending
        pending = tmp
        active.flip()
        pending.clear()
        this.notifyAll()
      }
    }
  }

  def finishedReading() {
    this.synchronized { finishedReadingUnsync() }
  }
} // End of Double Buffer


