package edu.berkeley.velox.util.zk

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.shared.{SharedValueReader, SharedValueListener, SharedValue}
import java.nio.ByteBuffer
import org.apache.curator.framework.state.ConnectionState
import edu.berkeley.velox.util.zk.DistributedCountdownLatch._
import com.typesafe.scalalogging.slf4j.Logging

/**
 * Created by crankshaw on 2/9/14. Much of this code was borrowed
 * from Apache Curator's SharedCount.java recipe
 */
class DistributedCountdownLatch(client: CuratorFramework, path: String) extends Logging {

  // value starts at 0
  val sharedValue: SharedValue = new SharedValue(client, path, toBytes(0))

  def start {
    sharedValue.start
  }

  def close {
    sharedValue.close
  }

  val myListener = new SharedValueListener
  { override def stateChanged(client: CuratorFramework, newState: ConnectionState) {
    // TODO do something here?
  }

    override def valueHasChanged(sharedValue: SharedValueReader, newValue: Array[Byte]) {
      synchronized {
        val currentCount = fromBytes(newValue)
        if (currentCount <= 0) {
          logger.debug(s"Counter now 0, notifying threads. $this")
          notifyFromWatcher()
        }
      }
    }
  }

  def reset(count: Int) {
    this.synchronized {
      sharedValue.setValue(toBytes(count))
    }
  }

  // will loop until decrements successfully
  def decrement() {
    synchronized {
      var value = fromBytes(sharedValue.getValue)
      logger.debug(s"counter value initially: $value")

      while (!sharedValue.trySetValue(toBytes(value - 1))) {
        value = fromBytes(sharedValue.getValue)
      }
      logger.debug(s"counter value now: ${value - 1}")
    }
  }

  // blocks until count drops to 0
  def awaitUntilZero() = {
    synchronized {
      // add a listener that waits for count to drop to 0 then blocks
      sharedValue.getListenable.addListener(myListener)
      logger.debug(s"About to start waiting on counter $this")
      wait()
      // remove all listeners once condition is reached
      logger.debug("Finished waiting on counter")
      logger.debug(s"SIZE BEFORE DELETING: ${sharedValue.getListenable.size()}")
      sharedValue.getListenable.removeListener(myListener)
      logger.debug(s"SIZE AFTER DELETING: ${sharedValue.getListenable.size()}")
    }
  }

  private def notifyFromWatcher() = {
    synchronized {
      notifyAll();
    }
  }

}

object DistributedCountdownLatch {
  def toBytes(value: Int): Array[Byte] = {
    val bytes = new Array[Byte](4)
    ByteBuffer.wrap(bytes).putInt(value)
    bytes
  }

  def fromBytes(bytes: Array[Byte]): Int = {
    ByteBuffer.wrap(bytes).getInt
  }
}