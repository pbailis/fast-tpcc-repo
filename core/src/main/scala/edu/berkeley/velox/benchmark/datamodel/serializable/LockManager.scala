package edu.berkeley.velox.benchmark.datamodel.serializable

import java.util.Collection;
import java.util.List;
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock
import java.util

class LockManager {
  var tableLatches: List[Lock] = new util.ArrayList[Lock]()
  var numBins: Int = 1000

  class LockRequest(val condition: Condition) {
    def sleep {
      condition.await
    }

    def wake {
      condition.signal
    }
  }

  class LockState {
    @volatile var held: Boolean = false
    val referenceCount = new AtomicInteger
    val lockLock = new ReentrantLock
    var queuedRequests: Collection[LockRequest] = null

    def markReference() {
      referenceCount.incrementAndGet()
    }

    def unmarkReference() {
      referenceCount.decrementAndGet()
    }

    def canRemove(): Boolean = {
      referenceCount.get() == 0
    }

    def acquire() {
      lockLock.lock()
      while (held) {
        val request = new LockRequest(lockLock.newCondition())
        if (queuedRequests == null) {
          queuedRequests = new util.ArrayList[LockRequest]
        }
        queuedRequests.add(request)
        request.sleep
        queuedRequests.remove(request)
      }

      held = true
      lockLock.unlock()
    }

    def release() {
      lockLock.lock()
      held = false
      if (queuedRequests != null && !queuedRequests.isEmpty) {
        queuedRequests.iterator().next().wake
      }
      lockLock.unlock()
    }
  }

  val lockTable = new ConcurrentHashMap[Any, LockState]()
  for (i <- 1 to numBins) {
    tableLatches.add(new ReentrantLock())
  }

  def getLatchForKey(key: Any): Lock = {
    tableLatches.get(Math.abs(key.hashCode() % numBins))
  }

  def readLock(key: Any) {
    // PDB TODO: FINISH!
  }

  def writeLock(key: Any) {
    // PDB TODO : FINISH!
  }

  def lock(key: Any) {
    val latch = getLatchForKey(key)
    latch.lock()
    var lockState = lockTable.get(key)
    if (lockState == null) {
      lockState = new LockState
      lockTable.put(key, lockState)
    }
    lockState.markReference()
    latch.unlock()
    lockState.acquire()
  }

  def unlock(key: Any) {
    val lockState = lockTable.get(key)
    lockState.release()
    lockState.unmarkReference()
    if (lockState.canRemove()) {
      getLatchForKey(key).lock()
      if (lockState.canRemove()) {
        lockTable.remove(key, lockState)
      }
      getLatchForKey(key).unlock()
    }
  }
}
