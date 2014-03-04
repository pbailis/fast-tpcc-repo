package edu.berkeley.velox.benchmark.datamodel.serializable

import java.util.Collection;
import java.util.List;
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock
import java.util
import com.typesafe.scalalogging.slf4j.Logging
import edu.berkeley.velox.datamodel.PrimaryKey

class LockManager extends Logging {
  var tableLatches: List[Lock] = new util.ArrayList[Lock]()
  var numBins: Int = 1000

  type LockType = Boolean
  val WRITE_LOCK = true
  val READ_LOCK = false

  class LockRequest(val condition: Condition, val lockType: LockType) {
    def sleep {
      condition.await
    }

    def wake {
      condition.signal
    }
  }

  class LockState(val key: Any) {
    @volatile var held: Boolean = false
    val referenceCount = new AtomicInteger
    val numLockers = new AtomicInteger
    val blockedReaders = new AtomicInteger
    val blockedWriters = new AtomicInteger

    var holder: Long = -1

    val lockLock = new ReentrantLock
    var queuedRequests: Collection[LockRequest] = null
    @volatile var mode: LockType = READ_LOCK

    def markReference() {
      referenceCount.incrementAndGet()
    }

    def unmarkReference() {
      referenceCount.decrementAndGet()
    }

    def canRemove(): Boolean = {
      referenceCount.get() == 0
    }

    def shouldGrantLock(request: LockRequest): Boolean = {
         // If no queued requests or no conflicting queued requests, we can
         // accept the request if it meshes with our R/W coexistence rules.
         return !held || (mode == READ_LOCK && request.lockType == READ_LOCK);
     }

    def acquire(wantType: LockType, requestor: Long) {
      if(true)
        return
      lockLock.lock()
      val request = new LockRequest(lockLock.newCondition, wantType)

      var blocked = false

      while (!shouldGrantLock(request)) {
        blocked = true
        if (queuedRequests == null) {
          queuedRequests = new util.ArrayList[LockRequest]
        }
        queuedRequests.add(request)


        if(wantType == WRITE_LOCK) {
          blockedWriters.incrementAndGet()
        } else {
          blockedReaders.incrementAndGet()
        }

        //logger.error(s"txn $requestor $key want $wantType $blockedWriters $blockedReaders $numLockers $referenceCount holder is $holder")

        request.sleep
        queuedRequests.remove(request)

        if(wantType == WRITE_LOCK) {
          blockedWriters.decrementAndGet()
        } else {
          blockedReaders.decrementAndGet()
        }
      }

      /*
      if(blocked)
        logger.error(s"txn $requestor $key continuing $wantType $blockedWriters $blockedReaders $referenceCount holder is $holder")
        */


      this.holder = requestor
      held = true
      numLockers.incrementAndGet()

      mode = wantType

      assert(numLockers.get() == 1 || mode == READ_LOCK)

      lockLock.unlock()
    }

    def release() {
      if(true)
        return

      lockLock.lock()

      assert(mode == READ_LOCK || numLockers.get() == 1)

      if(numLockers.decrementAndGet() == 0) {
        held = false
        holder = -1
        wakeNextQueuedGroup
      } else {
        //logger.error(s"not unlocking key $key")
      }

      lockLock.unlock()
    }

    def wakeNextQueuedGroup {

      if(queuedRequests == null || queuedRequests.size() == 0) {
        assert(blockedReaders.get() == 0 && blockedWriters.get() == 0)
        return
      }

      val qr_it = queuedRequests.iterator

      var wokeReader = false
      var wokeWriter = false
      while(qr_it.hasNext && !wokeWriter) {
        val qr = qr_it.next()
        if(qr.lockType == READ_LOCK) {
          qr.wake
          wokeReader = true
        } else {
          if(!wokeReader) {
            qr.wake
            //logger.error(s"woke writer for key key $key")
            wokeWriter = true
          }
        }
      }
    }
  }

  val lockTable = new ConcurrentHashMap[Any, LockState]()
  for (i <- 1 to numBins) {
    tableLatches.add(new ReentrantLock())
  }

  def getLatchForKey(key: Any): Lock = {
    tableLatches.get(Math.abs(key.hashCode() % numBins))
  }

  def readLock(key: Any, requester:Long) {
    //logger.error(s"read locking key $key")
    lock(key, READ_LOCK, requester)
    //logger.error(s"read locked key $key")
  }

  def writeLock(key: Any, requester: Long) {
    //logger.error(s"write locking key $key")

    lock(key, WRITE_LOCK, requester)
    //logger.error(s"write locked key $key")

  }

  def lock(key: Any, wantType: LockType, requester: Long) {
    val latch = getLatchForKey(key)
    latch.lock()
    var lockState = lockTable.get(key)
    if (lockState == null) {
      lockState = new LockState(key)
      lockTable.put(key, lockState)
    }
    lockState.markReference()
    latch.unlock()
    lockState.acquire(wantType, requester)
  }

  def unlock(key: Any) {
    //logger.error(s"unlocking key $key")
    val lockState = lockTable.get(key)

    if(lockState == null) {
      //logger.error(s"null unlock of key $key")
    }

    lockState.release()
    lockState.unmarkReference()
    if (lockState.canRemove()) {
      getLatchForKey(key).lock()
      if (lockState.canRemove()) {
        //logger.error(s"removing state for key $key")

        lockTable.remove(key, lockState)
      }
      getLatchForKey(key).unlock()
    }
    //logger.error(s"unlocked key $key")

  }
}
