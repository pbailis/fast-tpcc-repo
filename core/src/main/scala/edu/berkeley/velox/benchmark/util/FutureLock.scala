package edu.berkeley.velox.benchmark.util

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.LinkedBlockingQueue
import scala.concurrent.{Promise, Future}
import java.util
import com.typesafe.scalalogging.slf4j.Logging

class FutureLock extends Logging {
  val held = new AtomicBoolean
  val waiters = new LinkedBlockingQueue[Promise[Boolean]]

  def lock() : Future[Boolean] = {
    val p = Promise[Boolean]
    waiters.synchronized {
      if(!held.compareAndSet(false, true)) {
        waiters.add(p)
        logger.error(s"adding future $held")
      } else {
        logger.error("broke through $held!")
        p.success(true)
      }
    }
    p.future
  }

  def unlock() {
    waiters.synchronized {
      held.set(false)
      val waiter = waiters.poll()
      if(waiter != null) {
        waiter.success(true)
        held.set(true)
        logger.error(s"woke waiting waiter!")
      } else {
        logger.error(s"no one to wake $held")
      }
    }
  }
}
